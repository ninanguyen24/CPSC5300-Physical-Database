# 5300-Butterfly
## "Project Butterfly" - CPSC 5300 / 4300 Summer 2019
< < < SQL relation manager and parser program < < <

#### Sprint Verano:
Jonathan Kimray &
Grant Bishop
#### Sprint Otono:
Vishakha Bhavsar &
Alicia Mauldin
#### Sprint Invierno:
Nina Nguyen &
Maggie Dong

---

## Sprint Verano

### Milestone 1: Skeleton
	
	- Berkeley DB environment creation: handled by the 'Berkeley' class (Berkeley.h, .cpp)
		- Set up environment using constructor, i.e. Berkeley myDB("~/path/to/db/dir/")
		- Closes with destructor
		- Write and Read functions exist in header only 
		- Change for later milestones to integrate with storage engine!
	- Hyrise SQL query result unparsing: handled by the 'Execute' static class (Execute.h, .cpp)
		- Only one public function: Execute::getString(SQLParserResult*)
			- Doesn't support all Hyrise features, just a basic subset
		- Calls several private functions to unparse different elements of the parse tree
		- Test unparsing with TEST_Execute.cpp
			- Build with queryTest target, i.e. make queryTest (additional buld target in makefile)
			- Tests all queries shown on Milestone 1 canvas page, e.g. CREATE TABLE foo (a TEXT, b INT...
			- Some additional tests - not all are supposed to pass
	- sql5300 program: in main.cpp, default build target in Makefile
			- Running: provide directory for BerkeleyDB environment as command line argument
			- Sets up environment, then goes to SQL unparsing prompt using Execute

### Milestone 2: Rudimentary Storage Engine

#### To start: we are sorry
Our time managment was ... less than stellar.  The issue:

The program does compile, but it does not run.  the test_heap_storage function does not complete.  A segfault crashes the program.  We spent about 12 hours on Sunday (yeah poor time managment!) trying to figure out why.  We have an idea of where the probelm occurs - search for "SORRY" in heap_file.cpp to find comments associated with two functions explaining the issue so far.  Maybe fresh eyes will find the issue quickly?  We are going to office hours on Monday to get something worked out so that you, the next Team Butterfly won't be completely up a creek without a paddle.
Since we did not finish testing, there are LIKLEY ADDITIONAL BUGS.  

With appologies, 
Grant and Jon

#### Overview of code:

There isn't much to say here (since I have 15 min before midnight!): our code essentially follows the provided heap_storage.h and python examples without deviation (except for wherever our bug(s) are).

- storage engine.h: this is just the proivided file
- heap storage.h: this is almost just the provided file
- heap storage.cpp: our implemention.  Functions provided on canvas where used exactly as is
	- Implemention of other function follows very close to the python.
- main.cpp: nothing special here, quite similar to the provided sql5300.cpp

- Makefile: still has the additional queryTest target for milestone 1.
- MakefileDebug: this makefile has some additional options for debugging.  
	- Use with `make -f MakefileDebug [buildTarget]`
	- Additional arguments are passed to g++ to allow the use of the gdb debugger

This is what got us as far as we did in trying to find our problem.  In addition to the sql5300 build target there is a TEST_SP target.  This was used with TEST_SP.cpp when we where in the early stages of trying to locate the cause of our segfault.  

---

## Sprint Otono

### Milestone 3: Schema Storage

The following new files were provided to our team and uploaded to the repository:
- ParseTreeToString (.h header and .cpp files)
- schema_tables (.h header and .cpp files)
- SQLExec (.h header and .cpp files)
- storage_engine.cpp

#### Please note:
> Files provided to us by Professor Lundeen have replaced the previous versions of sql5300.cpp, storage_engine.h, heap_storage.cpp, heap_storage.h, and Makefile in order to fulfill requirements for Milestone 1 and 2. This was done as the original files could not compile or run without significant errors that would take too long to troubleshoot by our team this sprint.
>
> Some testing files were removed and similar functionality ported over to the main sql5300.cpp client portal.

We present a rudimentary implementation of the following functions found in SQLExec.cpp:
- CREATE TABLE
	- Takes the `create table [table] ([column] [type], ...)` argument and creates a database entry for a table
	- Only integers and text are currently supported column types
	- Attempts at creating a new table with a name already in use will result in an error
- DROP TABLE
	- Takes the `drop table [table]` argument and removes the corresponding database entry
	- Dropping nonexistent tables will result in an error
- SHOW TABLES
	- Takes the `show tables` argument and returns a list of tables in database
	- Returns 0 rows if no tables present
- SHOW COLUMNS
	- Takes the `show columns from [table]` argument and returns a list of columns present in a given table
	- Returns 0 rows if no columns present (table does not exist)

### Milestone 4: Indexing Setup

We made edits to SQLExec.cpp to incorporate new functionality for indices:
- CREATE INDEX
	- Takes the `create index [index] on [table] ([column], ...)` argument and creates a database entry for an index
	- The following attempts will result in errors:
		1. Creating an index on a nonexistent table and/or column
		2. Creating an index with a duplicate name as another index on a table
- SHOW INDEX
	- Takes the `show index from [table]` argument and returns a list of indices (including index type) present in a given table
	- Returns 0 rows if table does not exist or if table has no indices
- DROP INDEX
	- Takes the `drop index [index] from [table]` argument and removes corresponding references to it
	- Returns error if table does not exist or if index does not exist
- DROP TABLE
	- Updated to remove any related indices still present if table is dropped
	
The client portal file, sql5300.cpp, has also been updated to include easy testing calls for both Milestone 3 and 4.

#### Compiling and testing:
1. Use `make` to compile the program
	- Calling `make clean` followed by `make` will rebuild all file executables
2. Run the program executable with `./sql5300 [directory]`
	- Ensure a clean and writeable directory has been set up in the file directory
	- Reusing directories may cause unexpected behavior from Berkeley DB
3. When prompted by the `SQL>` command, enter a SQL query statement or one of the following keywords:
	- `quit`: exits the program and closes the database
	- `test`: performs Milestone 2 testing using the test_heap_storage() function
	- `test ms3`: performs Milestone 3 testing
	- `test ms4`: performs Milestone 4 testing

The current version is passing all Milestone 3 tests as shown:

```
$ ./sql5300 data
(sql5300: running with database environment at data)
SQL> test ms3

Query 1: show tables
SHOW TABLES
table_name
+----------+
successfully returned 0 rows

Query 2: show columns from _tables
SHOW COLUMNS FROM _tables
table_name column_name data_type
+----------+----------+----------+
"_tables" "table_name" "TEXT"
successfully returned 1 rows

Query 3: show columns from _columns
SHOW COLUMNS FROM _columns
table_name column_name data_type
+----------+----------+----------+
"_columns" "table_name" "TEXT"
"_columns" "column_name" "TEXT"
"_columns" "data_type" "TEXT"
successfully returned 3 rows

Query 4: create table foo (id int, data text, x integer, y integer, z integer)
CREATE TABLE foo (id INT, data TEXT, x INT, y INT, z INT)
created foo

Query 5: create table foo (goober int)
CREATE TABLE foo (goober INT)
Error: DbRelationError: foo already exists

Query 6: create table goo (x int, x text)
CREATE TABLE goo (x INT, x TEXT)
Error: DbRelationError: duplicate column goo.x

Query 7: show tables
SHOW TABLES
table_name
+----------+
"foo"
successfully returned 1 rows

Query 8: show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type
+----------+----------+----------+
"foo" "id" "INT"
"foo" "data" "TEXT"
"foo" "x" "INT"
"foo" "y" "INT"
"foo" "z" "INT"
successfully returned 5 rows

Query 9: drop table foo
DROP TABLE foo
dropped foo

Query 10: show tables
SHOW TABLES
table_name
+----------+
successfully returned 0 rows

Query 11: show columns from foo
SHOW COLUMNS FROM foo
table_name column_name data_type
+----------+----------+----------+
successfully returned 0 rows
```

The current version is passing all Milestone 4 tests as shown:

```
$ ./sql5300 data
(sql5300: running with database environment at data)
SQL> test ms4

Query 1: create table goober (x int, y int, z int)
CREATE TABLE goober (x INT, y INT, z INT)
created goober

Query 2: show tables
SHOW TABLES
table_name
+----------+
"goober"
successfully returned 1 rows

Query 3: show columns from goober
SHOW COLUMNS FROM goober
table_name column_name data_type
+----------+----------+----------+
"goober" "x" "INT"
"goober" "y" "INT"
"goober" "z" "INT"
successfully returned 3 rows

Query 4: create index fx on goober (x,y)
CREATE INDEX fx ON goober USING BTREE (x, y)
created index fx

Query 5: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
"goober" "fx" "x" 1 "BTREE" true
"goober" "fx" "y" 2 "BTREE" true
successfully returned 2 rows

Query 6: drop index fx from goober
DROP INDEX fx FROM goober
dropped index fx

Query 7: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
successfully returned 0 rows

Query 8: create index fx on goober (x)
CREATE INDEX fx ON goober USING BTREE (x)
created index fx

Query 9: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
"goober" "fx" "x" 1 "BTREE" true
successfully returned 1 rows

Query 10: create index fx on goober (y,z)
CREATE INDEX fx ON goober USING BTREE (y, z)
Error: DbRelationError: duplicate index goober fx

Query 11: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
"goober" "fx" "x" 1 "BTREE" true
successfully returned 1 rows

Query 12: create index fyz on goober (y,z)
CREATE INDEX fyz ON goober USING BTREE (y, z)
created index fyz

Query 13: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
"goober" "fx" "x" 1 "BTREE" true
"goober" "fyz" "y" 1 "BTREE" true
"goober" "fyz" "z" 2 "BTREE" true
successfully returned 3 rows

Query 14: drop index fx from goober
DROP INDEX fx FROM goober
dropped index fx

Query 15: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
"goober" "fyz" "y" 1 "BTREE" true
"goober" "fyz" "z" 2 "BTREE" true
successfully returned 2 rows

Query 16: drop index fyz from goober
DROP INDEX fyz FROM goober
dropped index fyz

Query 17: show index from goober
SHOW INDEX FROM goober
table_name index_name column_name seq_in_index index_type is_unique
+----------+----------+----------+----------+----------+----------+
successfully returned 0 rows

Query 18: drop table goober
DROP TABLE goober
dropped goober
```

---

## Sprint Invierno

### Milestone 5: Insert, Delete, Simple Queries

TODO

### Milestone 6: BTree Index

TODO
