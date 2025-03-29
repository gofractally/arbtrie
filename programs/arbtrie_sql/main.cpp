#include <sqlite3.h>
#include <arbtrie/database.hpp>
#include <iostream>
#include <string>

// Forward declaration for the SQLite module structure defined in arbtrie_vtab.cpp
extern "C" const sqlite3_module arbtrieModule;

// Forward declaration for the virtual table registration function
// extern "C" int sqlite3_arbtriemodule_init(sqlite3 *db);

void print_usage()
{
   std::cerr << "Usage: arbtrie_sql <arbtrie_db_path> [sql_command]" << std::endl;
}

// Basic SQLite error handling callback
int sql_callback(void* NotUsed, int argc, char** argv, char** azColName)
{
   for (int i = 0; i < argc; i++)
   {
      std::cout << azColName[i] << " = " << (argv[i] ? argv[i] : "NULL") << std::endl;
   }
   std::cout << std::endl;
   return 0;
}

int main(int argc, char* argv[])
{
   if (argc < 2)
   {
      print_usage();
      return 1;
   }

   std::string db_path = argv[1];
   sqlite3*    db;
   char*       zErrMsg = nullptr;
   int         rc;

   // Open SQLite database (in-memory for now, could use a file)
   rc = sqlite3_open(":memory:", &db);
   if (rc)
   {
      std::cerr << "Can't open database: " << sqlite3_errmsg(db) << std::endl;
      sqlite3_close(db);
      return 1;
   }
   std::cout << "SQLite in-memory database opened successfully." << std::endl;

   // Store the arbtrie database path in SQLite's user data (or manage globally)
   // This is a simple way; a more robust solution might use a context struct
   // REMOVED INCORRECT sqlite3_set_auxdata calls
   // sqlite3_set_auxdata(db, 0, "arbtrie_path", nullptr); // Key for aux data
   // sqlite3_set_auxdata(db, 0, (void*)db_path.c_str(), [](void* path_str) { /* no-op deleter */ });

   // Register the arbtrie virtual table module
   // Pass the database path string via the pClientData argument - NO LONGER NEEDED
   rc = sqlite3_create_module(db, "arbtrie", &arbtrieModule, nullptr /* pClientData */);
   // rc = sqlite3_arbtriemodule_init(db); // Old registration call - Removed
   if (rc != SQLITE_OK)
   {
      std::cerr << "Failed to register arbtrie virtual table module: " << sqlite3_errmsg(db)
                << std::endl;
      sqlite3_close(db);
      return 1;
   }
   std::cout << "Arbtrie virtual table module registered." << std::endl;

   // Example: Create a virtual table instance
   // The schema definition here is crucial and depends on how you map K/V to columns.
   // Add the path argument to the CREATE statement.
   std::string create_sql_str = "CREATE VIRTUAL TABLE kv USING arbtrie(path='" + db_path +
                                "', key TEXT PRIMARY KEY, value BLOB);";
   rc = sqlite3_exec(db, create_sql_str.c_str(), nullptr, 0, &zErrMsg);
   if (rc != SQLITE_OK)
   {
      std::cerr << "SQL error creating virtual table: " << zErrMsg << std::endl;
      sqlite3_free(zErrMsg);
      sqlite3_close(db);
      return 1;
   }
   std::cout << "Virtual table 'kv' created." << std::endl;

   if (argc == 3)
   {
      // Execute a single SQL command
      std::string sql_command = argv[2];
      std::cout << "Executing: " << sql_command << std::endl;
      rc = sqlite3_exec(db, sql_command.c_str(), sql_callback, 0, &zErrMsg);
      if (rc != SQLITE_OK)
      {
         std::cerr << "SQL error: " << zErrMsg << std::endl;
         sqlite3_free(zErrMsg);
      }
   }
   else
   {
      // Simple interactive shell (replace with a proper library like linenoise/readline for better UX)
      std::cout << "Enter SQL commands (terminate with ';', type 'exit' to quit):" << std::endl;
      std::string line;
      std::string command_buffer;
      while (true)
      {
         std::cout << "sql> ";
         std::getline(std::cin, line);
         if (line == "exit")
            break;

         command_buffer += line;
         if (!line.empty() && line.back() == ';')
         {
            rc = sqlite3_exec(db, command_buffer.c_str(), sql_callback, 0, &zErrMsg);
            if (rc != SQLITE_OK)
            {
               std::cerr << "SQL error: " << zErrMsg << std::endl;
               sqlite3_free(zErrMsg);
            }
            command_buffer.clear();
         }
         else
         {
            command_buffer += "\n";  // Add newline if command is not complete
         }
      }
   }

   sqlite3_close(db);
   std::cout << "Database connection closed." << std::endl;

   return 0;
}