/*
* heap_storage.cpp
* Authors: Nina Nguyen, Ashley DeCollibus, Kevin Lundeen
* Seattle University, CPSC 5300, Summer 2019
*
* SlottedPage : DbBlock
* HeapFile : DbFile
* HeapTable : DbRelation
*
* July 9, 2019
*/


#include "db_cxx.h"
#include "storage_engine.h"
#include "heap_storage.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>
#include <memory.h>
using namespace std;


/**
* @class SlottedPage - heap file implementation of DbBlock.
*
*      Manage a database block that contains several records.
Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
Record id are handed out sequentially starting with 1 as records are added with add().
Each record has a header which is a fixed offset from the beginning of the block:
Bytes 0x00 - Ox01: number of records
Bytes 0x02 - 0x03: offset to end of free space
Bytes 0x04 - 0x05: size of record 1
Bytes 0x06 - 0x07: offset to record 1
etc.
*
*/

typedef u_int16_t u16;


/******************SlottedPage protected functions implementation*************************/

//Constructor
//Paramaters: Block - page from the database that is using SlottedPage
//Paramaters: BlockID - id within DbFile
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
	if (is_new) {
		this->num_records = 0;
		this->end_free = DbBlock::BLOCK_SZ - 1;
		put_header();
	}
	else {
		get_header(this->num_records, this->end_free);
	}
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
	if (!has_room(data->get_size()))
		throw DbBlockNoRoomError("not enough room for new record");
	u16 id = ++this->num_records;
	u16 size = (u16)data->get_size();
	this->end_free -= size;
	u16 loc = this->end_free + 1;
	put_header();
	put_header(id, size, loc);
	memcpy(this->address(loc), data->get_data(), size);
	return id;
}

//Get a record from the block. Return none if it has been deleted
Dbt* SlottedPage::get(RecordID record_id) {
	u_int16_t size;
	u_int16_t loc;

	get_header(size, loc, record_id);
	if (loc == 0) {
		return nullptr;
	}
	return new Dbt(this->address(loc), size);
}

//Replace the record with the given data. Raises ValueError if it won't fit
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError) {
	u_int16_t size;
	u_int16_t loc;

	get_header(size, loc, record_id);

	u_int16_t new_size = (u_int16_t)data.get_size();

	if (new_size > size) {
		u_int16_t extra = new_size - size;
		if (!this->has_room(extra)) {
			throw DbBlockNoRoomError("not enough room for new record");
		}
		this->slide(loc, loc - extra);
		memcpy(this->address(loc - extra), data.get_data(), new_size);
	}
	else {
		memcpy(this->address(loc), data.get_data(), new_size);
		this->slide(loc + new_size, loc + size);
	}

	get_header(size, loc, record_id);
	put_header(record_id, new_size, loc);
}

//Mark the given record_id as deleted by changing its size to zero and its location to 0.
//Compact the rest of the data in the block. But keep the record ids the same for everyone.
void SlottedPage::del(RecordID record_id) {
	u_int16_t size;
	u_int16_t loc;

	get_header(size, loc, record_id);
	put_header(record_id, 0, 0);
	this->slide(loc, loc + size);
}

//Sequence of all non-deleted record ids.
RecordIDs* SlottedPage::ids(void) {
	u_int16_t size;
	u_int16_t loc;
	RecordIDs* temp = new RecordIDs;

	for (u_int16_t i = 1; i <= this->num_records; i++) {
		get_header(size, loc, i);
		if (loc != 0) {
			temp->push_back(i);
		}
	}
	return temp;
}

//Get the size and offset for given record_id. For record_id of zero, it is the block header
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
	size = get_n(4 * id);
	loc = get_n(4 * id + 2);
}

//Provided by Professor Lundeen
//Put the size and offset for given record_id. For record_id of zero, store the block header
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
	if (id == 0) { // called the put_header() version and using the default params
		size = this->num_records;
		loc = this->end_free;
	}
	put_n(4 * id, size);
	put_n(4 * id + 2, loc);
}

//Calculate if we have room to store a record with given size. The size should include the
//4 bytes for the header, too, if this is an add
bool SlottedPage::has_room(u_int16_t size) {
	u_int16_t available;
	available = this->end_free - (this->num_records + 2) * 4;
	return (size <= available);
}

/**If start < end, then remove data from offset start up to but not including offset end by
*sliding data that is to the left of start to the right. If start > end, then make room for
*extra data from end to start by sliding data that is to the left of start to the left.
*Also fix up any record headers whose data has slid. Assumes there is enough room if it is
*left shift (end < start).
*/
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
	u_int16_t shift;
	shift = end - start;
	if (shift == 0){
		return;
	}

	//slide data
	memcpy(this->address(this->end_free + 1 + shift), this->address(this->end_free + 1), shift);

	//fixup headers
	u_int16_t loc;
	u_int16_t size;
	//RecordIDs* temp = new RecordIDs();
	RecordIDs* temp = this->ids();

	for (unsigned int i = 0; i < temp->size(); i++) {
		get_header(size, loc, temp->at(i));
		if (loc <= start) {
			loc += shift;
			put_header(temp->at(i), size, loc);
		}
	}

	this->end_free += shift;
	this->put_header();
	delete temp;
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
	return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
	*(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
	return (void*)((char*)this->block.get_data() + offset);
}

/**************************Heap File Public Functions Implementation*********************/

/**
*Constructor is fully defined in header file. Don't need to replicate in cpp file
* @class HeapFile - Collection of blocks (implementation of DbFile)
*/

//Wrapper for Berkeley DB open, which does both open and creation
void HeapFile::db_open(uint flags) {
	if (!this->closed) {
		return;
	}


	this->db.set_re_len(DbBlock::BLOCK_SZ);
	const char* path = nullptr;
	_DB_ENV->get_home(&path);
	this->dbfilename = "./" + this->name + ".db"; //Get a db::open Is a directory otherwise
	this->db.open(nullptr, (this->dbfilename).c_str(), nullptr, DB_RECNO, flags, 0644);
	DB_BTREE_STAT *stat;
	this->db.stat(nullptr, &stat, DB_FAST_STAT);
	this->last = flags ? 0 : stat->bt_ndata;
	this->closed = false;
}

//Create physical File
void HeapFile::create(void) {
	db_open(DB_CREATE|DB_EXCL); // "|": OR binary operator
	SlottedPage* block = get_new();
	delete block;
}

//Delete the physical file
void HeapFile::drop(void) {
	close();
	Db db(_DB_ENV, 0);
	db.remove(this->dbfilename.c_str(), nullptr, 0);
}

//Open physical file
void HeapFile::open(void) {
	db_open();
}

//Close file
void HeapFile::close(void) {
	//this->write_lock = 1;
	db.close(0);
	closed = true;

}

//Get a block from the database file
SlottedPage* HeapFile::get(BlockID block_id) {
	Dbt key(&block_id, sizeof(block_id));
	Dbt data;

	this->db.get(nullptr, &key, &data, 0);
	return new SlottedPage(data, block_id, false);
}

//Provided by Professor Lundeen
//Returns the new empty DbBlock that is manging the records in this block and its block id
SlottedPage* HeapFile::get_new(void) {
	char block[DbBlock::BLOCK_SZ];
	std::memset(block, 0, sizeof(block));
	Dbt data(block, sizeof(block));

	int block_id = ++this->last;
	Dbt key(&block_id, sizeof(block_id));

	// write out an empty block and read it back in so Berkeley DB is managing the memory
	SlottedPage* page = new SlottedPage(data, this->last, true);
	this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
	this->db.get(nullptr, &key, &data, 0);
	return page;
}

//Write a block back to the database file
void HeapFile::put(DbBlock* block) {
	BlockID block_id = block->get_block_id();
	Dbt key(&block_id, sizeof(block_id));
	this->db.put(nullptr, &key, block->get_block(), 0);
}


//Sequence of all block ids
BlockIDs* HeapFile::block_ids() {
	BlockIDs* id = new BlockIDs();
	
	for (BlockID i = 1; i <= (this->last); i++)
		id->push_back(i);
	return id;
}


/**************************Heap Table Public Functions Implementation*********************/

/**
* @class HeapTable - Heap storage engine (implementation of DbRelation)
*/


HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
: DbRelation(table_name, column_names, column_attributes), file(table_name){}

//Execute: CREATE TABLE <table_name> ( <columns> )
//Is not responsible for metadata storage or validation
void HeapTable::create() {
	file.create();
}

//Execute: CREATE TABLE IF NOT EXISTS <table_name> ( <columns> )
//Is not responsible for metadata storage or validation
void HeapTable::create_if_not_exists() {
	try {
		open();
	}
	catch (DbException& e) {
		create();
	}
}

//Excecute: DROP TABLE <table_name>
void HeapTable::drop() {
	file.drop();
}

//Open existing table. Enables: insert, update, delete, select, project
void HeapTable::open() {
	file.open();
}

//Closes the table. Disables: insert, update, delete, select, project
void HeapTable::close() {
	file.close();
}

//Expect row to be a dictionary with column name keys.
//Execute: INSERT INTO <table_name> ( <row_keys> ) VALUES ( <row_values> )
//Return the handle of the inserted row
Handle HeapTable::insert(const ValueDict* row){
	open();
	//ValueDict* full = validate(row);
	Handle h = append(validate(row));
	return h;
}

//NOT SUPPORTED IN MILESTONE 1
/*Expect new_values to be a dictionary with column name keys.
Conceptually, execute: UPDATE INTO <table_name> SET <new_values> WHERE <handle>
where handle is sufficient to identify one specific record(e.g., returned from an insert
or select).
*/
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
	throw DbRelationError("Not Implemented");
}

//NOT SUPPORTED IN MILESTONE 1
/*Conceptually, execute: DELETE FROM <table_name> WHERE <handle>
where handle is sufficient to identify one specific record (e.g., returned from an insert
or select)
*/
void HeapTable::del(const Handle handle) {
	throw DbRelationError("Not Implemented");
}

/*Conceptually, execute: SELECT <handle> FROM <table_name>
If handles is specified, then use those as the base set of records to apply a refined selection to.
Returns a list of handles for qualifying rows.
*/
Handles* HeapTable::select() {
	Handles* handles = new Handles();
	BlockIDs* block_ids = file.block_ids();
	for (auto const& block_id : *block_ids) {
		SlottedPage* block = file.get(block_id);
		RecordIDs* record_ids = block->ids();
		for (auto const& record_id : *record_ids)
			handles->push_back(Handle(block_id, record_id));
		delete record_ids;
		delete block;
	}
	delete block_ids;
	return handles;
}

//NOTE SUPPORTED IN MILESTONE 1
/*Conceptually, execute: SELECT <handle> FROM <table_name> WHERE <where>
If handles is specified, then use those as the base set of records to apply a refined selection to.
Returns a list of handles for qualifying rows.
*/
Handles* HeapTable::select(const ValueDict* where) {
	//Handles* handles = new Handles();
	//BlockIDs* block_ids = file.block_ids();
	//for (auto const& block_id : *block_ids) {
	//	SlottedPage* block = file.get(block_id);
	//	RecordIDs* record_ids = block->ids();
	//	for (auto const& record_id : *record_ids)
	//		handles->push_back(Handle(block_id, record_id));
	//	delete record_ids;
	//	delete block;
	//}
	//delete block_ids;
	//return handles;
	throw DbRelationError("Not Implemented");
	return NULL;
}

//Return a ValueDict containing all data in a row
ValueDict* HeapTable::project(Handle handle) {
	this->open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file.get(block_id);
	Dbt* data = block->get(record_id);
	ValueDict* row = this->unmarshal(data);
	delete data;
	delete block;
	return row;
}

//Return a sequence of values for handle given by column_names
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names){
	this->open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file.get(block_id);
	Dbt* data = block->get(record_id);
	ValueDict* row = this->unmarshal(data);
	delete data;
	delete block;

	//This is to include column parameters. Can use Project(Handle handle) function above
	ValueDict* rowsToReturn = new ValueDict();
	for (auto const& column_name : *column_names) {
		//(rowsToReturn)[column_name] = (row)[column_name];
		(*rowsToReturn)[column_name] = (*row)[column_name];
		delete row;
	}
	return rowsToReturn;
}

//Check if the given row is acceptable to inser. Riase error if not.
//Otherwise return the full row dictionary
ValueDict* HeapTable::validate(const ValueDict* row) {
	ValueDict* full_row = new ValueDict();
	Value value;
	for (auto const& column_name : this->column_names) {
		//Use const iterator because ValueDict is a map<Identifier, Value>
		ValueDict::const_iterator column = row->find(column_name);
		if (column == row->end()) {
			throw DbRelationError("Not Yet Implemented");
		}
		else {
			value = column->second;
			(*full_row)[column_name] = value;
			//full_row.insert(pair<Identifier, Value>(column_name->first, column->second));
		}
	}
	return full_row;	
}

//Assumes row is fully fleshed-out. Appends a record to the file
Handle HeapTable::append(const ValueDict* row) {
	Dbt* data = marshal(row);
	//u_int32_t from heap_storage.h HeapFile
	SlottedPage* block = this->file.get(this->file.get_last_block_id());
	RecordID recordID;
	Handle result;
	try {
		recordID = block->add(data);
	}
	catch (DbBlockNoRoomError) {//From SlottedPage class put() function
		block = this->file.get_new();
		recordID = block->add(data);
	}
	this->file.put(block);
	delete[] (char*)data->get_data();
	delete block;
	result.first = file.get_last_block_id();
	result.second = recordID;
	return result;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
	char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
	uint offset = 0;
	uint col_num = 0;
	for (auto const& column_name : this->column_names) {
		ColumnAttribute ca = this->column_attributes[col_num++];
		ValueDict::const_iterator column = row->find(column_name);
		Value value = column->second;
		if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			*(int32_t*)(bytes + offset) = value.n;
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
			uint size = value.s.length();
			*(u16*)(bytes + offset) = size;
			offset += sizeof(u16);
			memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
			offset += size;
		}
		else {
			throw DbRelationError("Only know how to marshal INT and TEXT");
		}
	}
	char *right_size_bytes = new char[offset];
	memcpy(right_size_bytes, bytes, offset);
	delete[] bytes;
	Dbt *data = new Dbt(right_size_bytes, offset);
	return data;
}

//TODO
//Converts marshaled object back to original object type
ValueDict* HeapTable::unmarshal(Dbt* data) {
	ValueDict *row = new ValueDict();
	Value value;
	char *bytes = (char*)data->get_data();
	u16 offset = 0;
	u16 col_num= 0;
	for (auto const& column_name: this->column_names){
		ColumnAttribute ca = this->column_attributes[col_num++];
		value.data_type = ca.get_data_type();
		if(ca.get_data_type() == ColumnAttribute::DataType::INT){
			value.n = *(int32_t*)(bytes + offset);
			offset += sizeof(int32_t);
		}
		else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT){
			u16 size = *(u16*)(bytes + offset);
			offset += sizeof(u16);
			char buffer[DbBlock::BLOCK_SZ];
			memcpy(buffer, bytes + offset, size);
			buffer[size] = '\0';
			value.s = string(buffer);
			offset += size;
		}
		else {
			throw DbRelationError("Only know how to unmarshal INT and TEXT");
		}
		(*row)[column_name] = value;
	}
	return row;
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
	HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
	table1.create();
	std::cout << "create ok" << std::endl;
	table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
	std::cout << "drop ok" << std::endl;

	HeapTable table("_test_data_cpp", column_names, column_attributes);
	table.create_if_not_exists();
	std::cout << "create_if_not_exsts ok" << std::endl;

	ValueDict row;
	row["a"] = Value(12);
	row["b"] = Value("Hello!");
	std::cout << "try insert" << std::endl;
	table.insert(&row);
	std::cout << "insert ok" << std::endl;
	Handles* handles = table.select();
	std::cout << "select ok " << handles->size() << std::endl;
	ValueDict *result = table.project((*handles)[0]);
	std::cout << "project ok" << std::endl;
	Value value = (*result)["a"];
	if (value.n != 12)
		return false;
	value = (*result)["b"];
	if (value.s != "Hello!")
		return false;
	table.drop();

	return true;
}
