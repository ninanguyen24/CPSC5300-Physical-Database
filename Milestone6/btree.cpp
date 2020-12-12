/**
**@file btree.cpp - implementation for B+Tree
**@author Kevin Lundeen, Nina Nguyen
**@See "Seattle University, CPSC5300, Summer 2019"
**/

#include "btree.h"
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <iostream>
using namespace std;

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?! NINA
	build_key_profile();
}

//M6 - NINA
//Figure out the data types of each key component
void BTreeIndex::build_key_profile(){
	for (ColumnAttribute col: *relation.get_column_attributes(this->key_columns)){
		key_profile.push_back(col.get_data_type());
	}
}

//Destructor
BTreeIndex::~BTreeIndex() {
	// FIXME - free up stuff NINA
	delete this->stat;
	delete this->root;
	this->stat = nullptr;
	this->root = nullptr;
}

//Milestone 6 - NINA
// Create the index.
void BTreeIndex::create() {
	this->file.create();
	this->stat = new BTreeStat(file, STAT, STAT + 1, key_profile);
	this->root = new BTreeLeaf(file, stat->get_root_id(), key_profile, true);
	this->closed = false;

	//now build the index! -- add every row from relation into index
	Handles* handles = relation.select();
	Handles* n_handles = new Handles();

	for (auto const handle : *handles){
		insert(handle);
		n_handles->push_back(handle);
	}

	delete handles;
	delete n_handles;
}

//Milestone 6 - NINA
// Drop the index.
void BTreeIndex::drop() {
	file.drop();
}

//Milestone6 - NINA
// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
	if(this->closed){
		file.open();
		this->stat = new BTreeStat(file, STAT, key_profile);
		if (this->stat->get_height() == 1){
			this->root = new BTreeLeaf(file, stat->get_root_id(), key_profile, false);
		} else {
			this->root = new BTreeInterior(file, stat->get_root_id(), key_profile, false);
		}
		this->closed = false;
	}
}

//Milestone6 - NINA
// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
	file.close();
	this->stat = nullptr;
	this->root = nullptr;
	this->closed = true;
}

//Milestone 6 - MAGGIE
// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
    KeyValue* keyValue = this->tkey(key_dict);
    Handles* handles = this->_lookup(this->root, this->stat->get_height(), keyValue);
    delete keyValue;
    return handles;
}

//Milestone 6 - MAGGIE
// Helper for lookup, uses recursion to find the rows for the key.
// Returns list of found rows (empty if not found).
Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
    if (dynamic_cast<BTreeLeaf*>(node)) {
        // if at a tree leaf, can't go further down. Return the row if found or 
        // empty list if not found.
		Handle handle;
		Handles* handles = new Handles;
        try {
			handle = ((BTreeLeaf*) node)->find_eq(key);
        } catch (exception &e) {
            return handles; 
        }
		handles->push_back(handle);
        return handles;
    } else {
        // go down the tree by one layer to continue finding.
        return this->_lookup(((BTreeInterior*) node)->find(key, height), height - 1, key);
    }
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME: Not in scope for M6
}

//Milestone 6 - MAGGIE
// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
	// Get value to insert
    ValueDict *value_dict = this->relation.project(handle);
    KeyValue* tkey = this->tkey(value_dict);
    delete value_dict;

    // insert in index, if root split then add 1 to tree height
    Insertion split_root = this->_insert(this->root, this->stat->get_height(), tkey, handle);
    if (!BTreeNode::insertion_is_none(split_root)) {
        BlockID rroot = split_root.first;
        KeyValue boundary = split_root.second;

        // setup new root for tree
        BTreeInterior *newroot = new BTreeInterior(this->file, 0, this->key_profile, true);
        newroot->set_first(this->root->get_id());
        newroot->insert(&boundary, rroot);
        newroot->save();

        // set this tree root to the new root
        this->stat->set_root_id(newroot->get_id());
        this->stat->set_height(this->stat->get_height() + 1);
        this->stat->save();
        delete this->root;
        this->root = newroot;
    }
    delete tkey;
}

//Milestone 6 - MAGGIE
// Helper for insert, uses recursion to add to correct part of tree.
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
    Insertion result;
    if (dynamic_cast<BTreeLeaf*>(node)) {
        result = ((BTreeLeaf*) node)->insert(key, handle);
        ((BTreeLeaf*) node)->save();
        return result;
    } else {
        Insertion new_kid = this->_insert(((BTreeInterior*) node)->find(key, height), height - 1, key, handle);
        if (!BTreeNode::insertion_is_none(new_kid)) {
            result = ((BTreeInterior*) node)->insert(&new_kid.second, new_kid.first);
        }
        return result;
    }
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME: Not in scope for M6
}

//Milestone 6 - MAGGIE
KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
	KeyValue* keyvalue = new KeyValue;
    for (u_int i = 0; i < this->key_columns.size(); i++)
        keyvalue->push_back(key->at(key_columns[i]));
	return keyvalue;
}

//Milestone 6 - NINA
//Helper function to compare expect and returned results
bool test_btree(){
	cout << "test_btree started" << endl;
	bool result = true;
	ColumnNames column_names;
	column_names.push_back("a");
	column_names.push_back("b");
	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::INT);
	column_attributes.push_back(ca);
	
    HeapTable table("_test_create_drop_cpp", column_names, column_attributes);
    table.create();

	ValueDict row1;
	row1["a"] = Value(12);
	row1["b"] = Value(99);
	table.insert(&row1);

	
	ValueDict row2;
	row2["a"] = Value(88);
	row2["b"] = Value(101);
	table.insert(&row2);

	ColumnNames test_column_names;
	test_column_names.push_back("a");
	for(unsigned int i = 0; i <1000; i++){
		ValueDict batch_row;
		batch_row["a"] = i + 100;
		batch_row["b"] = ((-1)*i);
		table.insert(&batch_row);
	}

	DbIndex* index = new BTreeIndex(table, "testIndex", test_column_names, true);
	index->create();

	ValueDict test1, test2, test3, test4;
	
	//Test 1
	result = false;
	test1["a"] = Value(12);
	test1["b"] = Value(99);
	Handles* handles1 = index->lookup(&test1);
	if (handles1->empty()){
		result = false;
	} else {
		for (auto const& handle: *handles1){
			ValueDict* result_row = table.project(handle);
			if((*result_row)["a"] == test1["a"] && (*result_row)["b"] == test1["b"]){
				result = true;
				break;
			}
			delete result_row;
		}
	} delete handles1;

	//Test 2
	result = false;
	test2["a"] = Value(88);
	test2["b"] = Value(101);
	Handles* handles2 = index->lookup(&test2);
	if (handles2->empty()){
		result = false;
	} else {
		for (auto const& handle: *handles2){
			ValueDict* result_row = table.project(handle);
			if((*result_row)["a"] == test2["a"]&& (*result_row)["b"] == test2["b"]){
				result = true;
				break;
			}
			delete result_row;
		}
	} delete handles2;

	//Test 3
	result = false;
	test3["a"] = Value(6);
	Handles* handles3 = index->lookup(&test3);
	if (handles3->empty()){
		result = true;
	} else {
		for (auto const& handle: *handles3){
			ValueDict* result_row = table.project(handle);
			if((*result_row)["a"] == test3["a"]&& (*result_row)["b"] == test3["b"]){
				result = false;
				break;
			}
			delete result_row;
		}
	} delete handles3;

	//Test 4
	result = false;
	for (unsigned j = 1; j < 1000; j++){
		test4["a"] = Value(j + 100);
		test4["b"] = Value((-1)*j);
		Handles* handles4 = index->lookup(&test4);
		if(handles4->empty()){
			result = false;
		}
		else {
			for (auto const& handle : *handles4){
				ValueDict* result_row = table.project(handle);
				if((*result_row)["a"] == test4["a"]&& (*result_row)["b"] == test4["b"]){
					result = true;
					break;
				}
				delete result_row;
			}
		}
		delete handles4;
	}
	index->drop();
	delete index;
	table.drop();
	return result;
}

