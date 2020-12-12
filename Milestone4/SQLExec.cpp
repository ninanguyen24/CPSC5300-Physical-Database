/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen, Nina Nguyen, Celeste Broderick
 * @see "Seattle University, CPSC5300, Summer 2019"
 */
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr; //add for indices implementation

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name : *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row : *qres.rows) {
            for (auto const &column_name : *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                case ColumnAttribute::INT:
                    out << value.n;
                    break;
                case ColumnAttribute::TEXT:
                    out << "\"" << value.s << "\"";
                    break;
                case ColumnAttribute::BOOLEAN:
                    out << (value.n == 0 ? "false" : "true");
                    break;
                default:
                    out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

//Destructor
QueryResult::~QueryResult() {
    if (column_names != nullptr) {
        delete column_names;
    }
    if (column_attributes != nullptr) {
        delete column_attributes;
    }
    if (rows != nullptr) {
        for (auto row : *rows) {
            delete row;
        }
        delete rows;
    }
}

//Execute query
QueryResult *SQLExec::execute(const SQLStatement *statement)
        throw (SQLExecError) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
    }

    //initialize _indices if not yet present
    if (SQLExec::indices == nullptr) {
        SQLExec::indices = new Indices();
    }

    try {
        switch (statement->type()) {
        case kStmtCreate:
            return create((const CreateStatement *) statement);
        case kStmtDrop:
            return drop((const DropStatement *) statement);
        case kStmtShow:
            return show((const ShowStatement *) statement);
        default:
            return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// Pull out column name and attributes from AST's column definition clause
void SQLExec::column_definition(const ColumnDefinition *col,
        Identifier& column_name, ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
    case ColumnDefinition::TEXT:
        column_attribute.set_data_type(ColumnAttribute::TEXT);
        break;
    case ColumnDefinition::INT:
        column_attribute.set_data_type(ColumnAttribute::INT);
        break;
    default:
        throw SQLExecError("Doesn't support this data type");
    }
}

// CREATE... Take in CreateStatement from user, and return QueryResult based on type of CreateStatement
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
    case CreateStatement::kTable:
        return create_table(statement);
    case CreateStatement::kIndex:
        return create_index(statement);
    default:
        return new QueryResult(
                "Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

// CREATE TABLE... Create a new table based on CreateStatement
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    //Declaring variables
    Identifier tableName = statement->tableName;
    Identifier column_name;
    ColumnNames column_names;
    ColumnAttribute column_attribute;
    ColumnAttributes column_attributes;
    ValueDict row;

    //Update _tables schema
    row["table_name"] = tableName;
    Handle tHandle = SQLExec::tables->insert(&row);

    //Get columns for _columns schema update
    for (ColumnDefinition* column : *statement->columns) {
        column_definition(column, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    //update _columns schema
    try {
        Handles cHandles;
        DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);

        try {
            for (unsigned int i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(
                        column_attributes[i].get_data_type()
                                == ColumnAttribute::INT ? "INT" : "TEXT");
                cHandles.push_back(cols.insert(&row));
            }
            //Create the table relation
            DbRelation& table = SQLExec::tables->get_table(tableName);
            if (statement->ifNotExists) {
                table.create_if_not_exists();
            } else {
                table.create();
            }
        } catch (exception& e) {
            //try to remove from _columns
            try {
                for (unsigned int i = 0; i < cHandles.size(); i++) {
                    cols.del(cHandles.at(i));
                }
            } catch (...) {
            }
            throw;
        }
    } catch (exception& e) {
        try {
            // try to remove from _tables
            SQLExec::tables->del(tHandle);
        } catch (...) {
        }
        throw;
    }
    return new QueryResult("created " + tableName);
}

// CREATE INDEX... Create a new index for the table specified by user
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    //declare Identifier
    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;
    Identifier index_type;

    bool is_unique;

    //Add to schema
    Handles cHandles;
    ValueDict row;
    row["table_name"] = table_name;

    try {
        index_type = statement->indexType;
    } catch (exception& e) {
        index_type = "BTREE";
    }

    if (index_type == "BTREE") {
        is_unique = true;
    } else {
        is_unique = false;
    }

    row["table_name"] = table_name;
    row["index_name"] = index_name;
    row["seq_in_index"] = 0;
    row["index_type"] = index_type;
    row["is_unique"] = is_unique;

    try {
        //int i = 0;
        for (auto const& col : *statement->indexColumns) {
            row["seq_in_index"].n += 1;
            row["column_name"] = string(col);
            cHandles.push_back(SQLExec::indices->insert(&row));
        }
        //create index
        DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
        index.create();
    }
    //delete all the handles if error occurs
    catch (exception& e) {
        for (auto const &handle : cHandles) {
            SQLExec::indices->del(handle);
        }
        throw;
    }

    return new QueryResult("created index " + index_name);
}

// DROP... Take in DropStatement from user, and return QueryResult based on type of DropStatement
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch (statement->type) {
    case DropStatement::kTable:
        return drop_table(statement);
    case DropStatement::kIndex:
        return drop_index(statement);
    default:
        return new QueryResult(
                "Only DROP TABLE and CREATE INDEX are implemented");
    }
}

// DROP TABLE...
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    //M4: added
    //Check to see if statement is valid
    if (statement->type != DropStatement::kTable) {
        throw SQLExecError("unrecognized DROP statement");
    }

    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);

    //M4: added
    //get indices table and handle
    DbRelation& indicesTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handles* h_indices = indicesTable.select(&where);
    //Get name of indices
    IndexNames indexID = SQLExec::indices->get_index_names(table_name);

    //Deleting indices from relation
    for (auto const& handle : *h_indices)
        indicesTable.del(handle);

    //getting index name and dropping them
    for (auto const& index_id : indexID) {
        DbIndex& index = SQLExec::indices->get_index(table_name, index_id);
        index.drop();
    }

    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = columns.select(&where);
    //delete from relation
    for (auto const& handle : *handles) {
        columns.del(handle);
    }

    delete h_indices; //delete handle to prevent memory leak
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(string("dropped ") + table_name);
}

// DROP INDEX... Remove all the rows from _indices for this index
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    //Double check if statement is valid DROP
    if (statement->type != DropStatement::kIndex) {
        return new QueryResult("Unrecognized DROP statement");
    }

    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    ValueDict where;
    where["table_name"] = table_name;
    where["index_name"] = index_name;

    Handles* index_handles = SQLExec::indices->select(&where);
    index.drop();

    for (unsigned int i = 0; i < index_handles->size(); i++) {
        SQLExec::indices->del(index_handles->at(i));
    }

    //clear up memory
    delete index_handles;
    return new QueryResult("dropped index " + index_name);
}

// Executes ShowStatement
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
    case ShowStatement::EntityType::kTables:
        return show_tables();
    case ShowStatement::EntityType::kColumns:
        return show_columns(statement);
    case ShowStatement::EntityType::kIndex:
        return show_index(statement);
    default:
        throw SQLExecError("Unknown SHOW type");
    }
}

// SHOW INDEX... Retrieves all index details from database indices schema
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    // Set up labels/header
    ColumnNames *colNames = new ColumnNames();
    colNames->push_back("table_name");
    colNames->push_back("index_name");
    colNames->push_back("seq_in_index");
    colNames->push_back("column_name");
    colNames->push_back("index_type");
    colNames->push_back("is_unique");
    ColumnAttributes *colAttr = new ColumnAttributes();
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Identify index to show
    Identifier tableName = statement->tableName;
    ValueDict where;
    where["table_name"] = Value(tableName);

    // Get rows related to this index from indices schema
    Handles* handles = SQLExec::indices->select(&where);
    unsigned int size = handles->size();
    ValueDicts *rows = new ValueDicts;
    for (auto const &hd : *handles) {
        ValueDict *row = SQLExec::indices->project(hd, colNames);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(colNames, colAttr, rows,
            "successfully returned " + to_string(size) + " rows");
}

// SHOW TABLES... Retrieves all table names from database table schema
// (Does not return names of indices, tables, or columns schemas)
QueryResult *SQLExec::show_tables() {
    // Set up labels/header
    ColumnNames *colNames = new ColumnNames;
    colNames->push_back("table_name");
    ColumnAttributes *colAttr = new ColumnAttributes;
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Retrieve handles from schema and look up table names
    ValueDicts *tableNames = new ValueDicts();
    Handles *myHandles = SQLExec::tables->select();

    // Get size/number of rows (subtract three to account for tables, columns, and indices schemas)
    unsigned int size = myHandles->size() - 3;
    for (auto const &h : *myHandles) {
        ValueDict *row = SQLExec::tables->project(h, colNames);
        Identifier myName = row->at("table_name").s;
        if (myName != Tables::TABLE_NAME && myName != Columns::TABLE_NAME
                && myName != Indices::TABLE_NAME)
            tableNames->push_back(row);
    }
    delete myHandles;
    return new QueryResult(colNames, colAttr, tableNames,
            "successfully returned " + to_string(size) + " rows");
}

// Retrieves column details for a the table specified by ShowStatement
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation &myCols = SQLExec::tables->get_table(Columns::TABLE_NAME);

    // Set up labels/header
    ColumnNames *colNames = new ColumnNames();
    colNames->push_back("table_name");
    colNames->push_back("column_name");
    colNames->push_back("data_type");
    ColumnAttributes *colAttr = new ColumnAttributes();
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Retrieve handles for this table from columns schema
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles *myHandles = myCols.select(&where);
    unsigned int size = myHandles->size();
    ValueDicts *rows = new ValueDicts;
    for (auto const &h : *myHandles) {
        ValueDict* row = myCols.project(h, colNames);
        rows->push_back(row);
    }
    delete myHandles;
    return new QueryResult(colNames, colAttr, rows,
            "successfully returned " + to_string(size) + " rows");
}
