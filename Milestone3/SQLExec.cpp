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

QueryResult *SQLExec::create(const CreateStatement *statement) {
    //Checking to see if statement is CREATE
    if (statement->type != CreateStatement::kTable) {
        return new QueryResult("This only handle CREATE TABLE");
    }

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

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    //Checking to see if statement is DROP
    if (statement->type != DropStatement::kTable) {
        return new QueryResult("This only handle DROP TABLE");
    }

    Identifier tableName = statement->name;

    //Check request to make sure it's not schema tables
    if (tableName == Tables::TABLE_NAME || tableName == Columns::TABLE_NAME) {
        throw SQLExecError("Cannot drop a schema table!");
    }

    DbRelation& table = SQLExec::tables->get_table(tableName); //Get the table

    //Remove _columns schema table
    ValueDict where;
    where["table_name"] = Value(tableName);
    DbRelation& col = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* hd = col.select(&where);
    for (unsigned int i = 0; i < hd->size(); i++) {
        col.del(hd->at(i));
    }

    delete hd; //Handle memory that was declared in heap
    table.drop(); //drop table

    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());
    return new QueryResult("dropped " + tableName);
}

// Executes ShowStatement
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
    case ShowStatement::EntityType::kTables:
        return show_tables();
    case ShowStatement::EntityType::kColumns:
        return show_columns(statement);
    case ShowStatement::EntityType::kIndex: //Index to be supported in future milestone
        throw SQLExecError("Index not yet supported");
    default:
        throw SQLExecError("Unknown type");
    }
}

// Retrieves all table names from database table schema
// (Does not return names of tables or columns schemas)
QueryResult *SQLExec::show_tables() {
    // Set up labels/header
    ColumnNames *colNames = new ColumnNames;
    colNames->push_back("table_name");
    ColumnAttributes *colAttr = new ColumnAttributes;
    colAttr->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // Retrieve handles from schema and look up table names
    ValueDicts *tableNames = new ValueDicts();
    Handles *myHandles = SQLExec::tables->select();
    unsigned int size = myHandles->size() - 2;
    for (auto const &h : *myHandles) {
        ValueDict *row = SQLExec::tables->project(h, colNames);
        Identifier myName = row->at("table_name").s;
        if (myName != Tables::TABLE_NAME && myName != Columns::TABLE_NAME)
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
