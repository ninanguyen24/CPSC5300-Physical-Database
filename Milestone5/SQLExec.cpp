/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen, Nina Nguyen
 * @see "Seattle University, CPSC5300, Summer 2019"
 */
#include <algorithm>
#include "SQLExec.h"
#include "EvalPlan.h"
#include "ParseTreeToString.h"

using namespace std;
using namespace hsql;

Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
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

QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

//Pull out conjunctions of equality predicates from parse tree
ValueDict* SQLExec::get_where_conjunction(const hsql::Expr *expr, const ColumnNames *col_names){
    ValueDict* rows = new ValueDict;
    if (expr->type == kExprOperator){ //check for WHERE clause
        if(expr->opType == Expr::AND){
            ValueDict* sub = get_where_conjunction(expr->expr, col_names); //recursively get left
            if (sub != nullptr){
                rows->insert(sub->begin(), sub->end());
            }
            sub = get_where_conjunction(expr->expr2, col_names);
            rows->insert(sub->begin(), sub->end());
        }
        else if(expr->opType == Expr::SIMPLE_OP){
            if(expr->opChar == '='){//handles equality in statement
                Identifier col = expr->expr->name;
                if(find(col_names->begin(), col_names->end(), col) == col_names->end()){//look for column
                    throw DbRelationError("unknown column '" + col + "'");
                }
                switch(expr->expr2->type){
                    case kExprLiteralString: {
                        rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->name)));
                        break;
                    }
                    case kExprLiteralInt: {
                        rows->insert(pair<Identifier, Value>(col, Value(expr->expr2->ival)));
                        break;
                    }
                    default:
                        throw DbRelationError("Not valid data type");
                }
            }
            else {
                throw DbRelationError("only equality predicates currently supported");
            }
        }
        else {
            throw DbRelationError("only support AND conjunctions");
        }
        return rows;
    } else {
        throw DbRelationError("Invalid Operator");
    }
}

QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
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
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

//Milestone5 - Nina
//Construct ValueDict row to insert then insert it. Add indices as well
QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier table_name = statement->tableName; //get table name
    DbRelation& table = SQLExec::tables->get_table(table_name); //get table
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    ValueDict row;
    Handle insertHandle;
    unsigned int index = 0;

    //get column info
    if(statement->columns != nullptr){
        for (auto const col : *statement->columns){
            column_names.push_back(col);
        }
    }
    else {
        for (auto const col: table.get_column_names()){
            column_names.push_back(col);
        }
    }

    //insert statement into row
    for (auto const& col : *statement->values){
        switch(col->type){
            case kExprLiteralString:
                //row[col[index]] = Value(col->name);
                row[column_names[index]] = Value(col->name);
                index++;
                break;
            case kExprLiteralInt:
                row[column_names[index]] = Value(col->ival);
                index++;
                break;
            default:
            //Don't add to table
                throw SQLExecError("Insert can only handle INT or Text");
        }
    }

    insertHandle = table.insert(&row); //add insert to handle
    IndexNames index_names = SQLExec::indices->get_index_names(table_name); //get index names
    for(Identifier i_name : index_names){
        DbIndex& index = SQLExec::indices->get_index(table_name, i_name);
        index.insert(insertHandle);
    }
    //get query index info
    return new QueryResult("Successfully inserted 1 row into " 
    + table_name + " and " + to_string(index_names.size()) + " indices"); //FIXME
}

//Milestone5 - NINA
//Delete row and any indices
QueryResult *SQLExec::del(const DeleteStatement *statement) {
    Identifier table_name = statement->tableName; //get table name
    DbRelation& table = SQLExec::tables->get_table(table_name); //get table using tablename, and where clause
    ColumnNames col_names;

    for (auto const col: table.get_column_names()){
        col_names.push_back(col);
    }

    //make the evaluation plan
    EvalPlan *plan = new EvalPlan(table); 

    //This is to delete with or without where clause
    ValueDict* where = new ValueDict;
    if (statement->expr != NULL){
        try{
            where = get_where_conjunction(statement->expr, &col_names);
        }
        catch (exception &e){
            throw;
        }
        plan = new EvalPlan(where, plan);// define eval plan with where clause
    }
    
    //execute evalutation plan to get list of handles
    EvalPlan *opt = plan->optimize();
    EvalPipeline pipeline = opt->pipeline();
    //EvalPipeline t = optimized->pipeline();
    //Handles *handles = pipeline;
    Handles *handles = pipeline.second;

    //Remove from indices
    auto index_names = SQLExec::indices->get_index_names(table_name);
    unsigned int handle_size = handles->size();
    unsigned int index_size = index_names.size();
    for( auto const &handle: *handles){
        for (unsigned int i = 0; i < index_names.size(); i++){
            DbIndex &index = SQLExec::indices->get_index(table_name, index_names[i]);
            index.del(handle);
        }
    }

    //removing from table
    for (auto const& handle: *handles){
        table.del(handle);
    }
    delete where; //clear up memory
    return new QueryResult("successfully deleted " + to_string(handle_size) 
    + " rows from " + table_name + " and " + to_string(index_size) + " indices");
    
    //return new QueryResult("DELETE statement not yet implemented");  // FIXME Nina
}

//Milestone 5 - MAGGIE
QueryResult *SQLExec::select(const SelectStatement *statement) {
    Identifier tbname = statement->fromTable->name;//get table name
    DbRelation& table = SQLExec::tables->get_table(tbname);
    ColumnNames* col_names = new ColumnNames;

    //get column names
    for (auto const &expr : *statement->selectList) {
        switch (expr->type) {
            case kExprStar:
            break;
            case kExprColumnRef:
                col_names->push_back(expr->name);
                break;
            default:
                return new QueryResult("Invalid select statement");
        }
    }

    //get column in Select *
    if (col_names->empty()) {
        for (auto const col : table.get_column_names()) {
        col_names->push_back(col);
        }
    }


    //Start base of plan at a TableScan
    EvalPlan *plan = new EvalPlan(table);

    //Enclose that in a Select if we have a where clause
    ValueDict* where = new ValueDict;
    if (statement->whereClause != NULL) {
        where = get_where_conjunction(statement->whereClause, &table.get_column_names());
        plan = new EvalPlan(where, plan);
    }

    //ProjectAll or a Project
    plan = new EvalPlan(col_names, plan);

    //Optimize the plan and evaluate the optimized plan
    EvalPlan *optimized = plan->optimize();
    ValueDicts* rows = optimized->evaluate();

    //Handle memory
    delete where;

    return new QueryResult(col_names, NULL,
    rows, "successfully returned " + to_string(rows->size()) + " rows");  
   //return new QueryResult("SELECT statement not yet implemented");  // FIXME Maggie
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}
 
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (...) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception& e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier index_name = statement->indexName;
    Identifier table_name = statement->tableName;

    // get underlying relation
    DbRelation& table = SQLExec::tables->get_table(table_name);

    // check that given columns exist in table
    const ColumnNames& table_columns = table.get_column_names();
    for (auto const& col_name: *statement->indexColumns)
        if (find(table_columns.begin(), table_columns.end(), col_name) == table_columns.end())
            throw SQLExecError(string("Column '") + col_name + "' does not exist in " + table_name);

    // insert a row for every column in index into _indices
    ValueDict row;
    row["table_name"] = Value(table_name);
    row["index_name"] = Value(index_name);
    row["index_type"] = Value(statement->indexType);
    row["is_unique"] = Value(string(statement->indexType) == "BTREE"); // assume HASH is non-unique --
    int seq = 0;
    Handles i_handles;
    try {
        for (auto const &col_name: *statement->indexColumns) {
            row["seq_in_index"] = Value(++seq);
            row["column_name"] = Value(col_name);
            i_handles.push_back(SQLExec::indices->insert(&row));
        }

        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();

    } catch (...) {
        // attempt to remove from _indices
        try {  // if any exception happens in the reversal below, we still want to re-throw the original ex
            for (auto const &handle: i_handles)
                SQLExec::indices->del(handle);
        } catch (...) {}
        throw;  // re-throw the original exception (which should give the client some clue as to why it did
    }
    return new QueryResult("created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}
 
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);

    // remove any indices
    for (auto const& index_name: SQLExec::indices->get_index_names(table_name)) {
        DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
        index.drop();  // drop the index
    }
    Handles* handles = SQLExec::indices->select(&where);
    for (auto const& handle: *handles)
        SQLExec::indices->del(handle);  // remove all rows from _indices for each index on this table
    delete handles;

    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    handles = columns.select(&where);
    for (auto const& handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(string("dropped ") + table_name);
}

QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    // drop index
    DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
    index.drop();

    // remove rows from _indices for this index
    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);
    Handles* handles = SQLExec::indices->select(&where);
    for (auto const& handle: *handles)
        SQLExec::indices->del(handle);
    delete handles;

    return new QueryResult("dropped index " + index_name);
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    ColumnNames* column_names = new ColumnNames;
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_names->push_back("table_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("index_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("column_name");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("seq_in_index");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));

    column_names->push_back("index_type");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    column_names->push_back("is_unique");
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    ValueDict where;
    where["table_name"] = Value(string(statement->tableName));
    Handles* handles = SQLExec::indices->select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::indices->project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_tables() {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME
            && table_name != Columns::TABLE_NAME
            && table_name != Indices::TABLE_NAME) {

             	rows->push_back(row);
        }
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

