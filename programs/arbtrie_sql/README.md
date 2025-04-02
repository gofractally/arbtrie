# Arbtrie SQL Server

This program provides a basic SQL interface to an Arbtrie key-value store using SQLite's virtual table mechanism.

## Building

Assuming you have CMake, a C++ compiler, and the SQLite3 development libraries installed, you can build this program from the project's root directory:

```bash
# Configure the build (from the project root)
cmake -S . -B build/debug -DBUILD_TESTING=ON 

# Build the arbtrie_sql executable and tests
cmake --build build/debug --target arbtrie_sql arbtrie_sql_tests -j
```

This will place the executable `arbtrie_sql` and the test executable `arbtrie_sql_tests` in the `build/debug/bin` directory (or similar, depending on your CMake setup).

## Running Tests

To run the unit tests:

```bash
cd build/debug
ctest -V
```

## Usage

The `arbtrie_sql` executable takes the path to the Arbtrie database directory as the first argument. It can optionally take a single SQL command string as the second argument to execute immediately. If no SQL command is provided, it enters a simple interactive SQL shell.

**Creating/Opening an Arbtrie Database and Running Interactively:**

```bash
./build/debug/bin/arbtrie_sql ./my_arbtrie_db
```

This will:
1. Create the directory `./my_arbtrie_db` if it doesn't exist.
2. Open (or create) the Arbtrie database within that directory.
3. Create an in-memory SQLite database and register the Arbtrie virtual table module.
4. Automatically execute `CREATE VIRTUAL TABLE kv USING arbtrie(key TEXT PRIMARY KEY, value BLOB);` (or similar, depending on `main.cpp`) to map the Arbtrie store.
5. Start an interactive SQL prompt (`sql>`).

You can then type standard SQL commands terminated by a semicolon (`;`). Type `exit` to quit.

**Example Interactive Session:**

```
./build/debug/bin/arbtrie_sql ./my_arbtrie_db
SQLite in-memory database opened successfully.
Opened arbtrie database at: ./my_arbtrie_db
Arbtrie virtual table module registered.
Virtual table 'kv' created.
Enter SQL commands (terminate with ';', type 'exit' to quit):
sql> INSERT INTO kv (key, value) VALUES ('mykey', 'myvalue');
sql> SELECT * FROM kv WHERE key = 'mykey';
key = mykey
value = myvalue

sql> UPDATE kv SET value = 'new_value' WHERE key = 'mykey';
sql> SELECT value FROM kv WHERE key = 'mykey';
value = new_value

sql> DELETE FROM kv WHERE key = 'mykey';
sql> SELECT * FROM kv;
sql> exit
Database connection closed.
```

**Executing a Single SQL Command:**

```bash
./build/debug/bin/arbtrie_sql ./my_arbtrie_db "INSERT INTO kv (key, value) VALUES ('another_key', X'0123ABCD');"
```

This will execute the specified SQL command and print the results (if any) before exiting.

**Notes:**

*   The current implementation in `main.cpp` hardcodes the virtual table creation as `CREATE VIRTUAL TABLE kv USING arbtrie(key TEXT PRIMARY KEY, value BLOB);`. Modify `main.cpp` if you need a different table name or schema upon startup.
*   The interactive shell is very basic. For more advanced features, consider integrating libraries like `linenoise` or `readline`.
*   Error handling in the virtual table implementation (`arbtrie_vtab.cpp`) is basic. Robust error reporting from Arbtrie operations back to SQLite is needed for production use. 