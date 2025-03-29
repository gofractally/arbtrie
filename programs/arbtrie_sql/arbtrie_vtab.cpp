#define SQLITE_CORE 1  // Ensure SQLite core features are enabled
#include <sqlite3.h>
#include <sqlite3ext.h>
#include <arbtrie/database.hpp>
#include <arbtrie/iterator.hpp>  // Assuming iterator definitions are here
#include <arbtrie/transaction.hpp>
#include <cassert>  // For assert()
#include <cstring>
#include <iomanip>  // Required for std::hex, std::setw, std::setfill
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>  // Required for std::ostringstream
#include <string>
#include <system_error>
#include <variant>
#include <vector>

// --- Utility Functions ---

// Helper function to convert byte data to a hex string for logging
std::string to_hex_string(const unsigned char* data, size_t len)
{
   if (!data || len == 0)
      return "<empty_or_null>";
   std::ostringstream oss;
   oss << std::hex << std::setfill('0');
   for (size_t i = 0; i < len; ++i)
   {
      oss << std::setw(2) << static_cast<int>(data[i]);
   }
   return oss.str();
}

// Add back the value_view overload, assuming it's a distinct type or has different constness needs
std::string to_hex_string(arbtrie::value_view v)
{
   return to_hex_string(reinterpret_cast<const unsigned char*>(v.data()), v.size());
}

std::string to_hex_string(const std::vector<char>& vec)
{
   return to_hex_string(reinterpret_cast<const unsigned char*>(vec.data()), vec.size());
}

arbtrie::key_view to_key_view(const unsigned char* str)
{
   return str ? arbtrie::key_view(reinterpret_cast<const char*>(str),
                                  std::strlen(reinterpret_cast<const char*>(str)))
              : arbtrie::key_view();
}

arbtrie::key_view to_key_view(const char* str, int len)
{
   return arbtrie::key_view(reinterpret_cast<const arbtrie::byte_type*>(str), len);
}
arbtrie::value_view to_value_view(const unsigned char* data, int len)
{
   return arbtrie::value_view(reinterpret_cast<const arbtrie::byte_type*>(data), len);
}

// Helper function to determine SQLite type from string (case-insensitive)
int get_sqlite_type_from_string(const std::string& type_str)
{
   std::string upper_type;
   std::transform(type_str.begin(), type_str.end(), std::back_inserter(upper_type), ::toupper);

   if (upper_type.find("TEXT") != std::string::npos)
      return SQLITE_TEXT;
   if (upper_type.find("INTEGER") != std::string::npos)
      return SQLITE_INTEGER;
   if (upper_type.find("REAL") != std::string::npos)
      return SQLITE_FLOAT;
   if (upper_type.find("FLOAT") != std::string::npos)
      return SQLITE_FLOAT;
   if (upper_type.find("DOUBLE") != std::string::npos)
      return SQLITE_FLOAT;
   // Default to BLOB for unrecognized or explicit BLOB types
   return SQLITE_BLOB;
}

// --- Virtual Table Structures ---

// Represents the virtual table itself (shared across connections)
struct arbtrie_vtab
{
   sqlite3_vtab                            base;  // Base class for virtual tables
   sqlite3*                                db;    // The SQLite database connection
   std::string                             arbtrie_db_path;
   std::shared_ptr<arbtrie::database>      arbtrie_db;      // Keep the database object
   std::shared_ptr<arbtrie::write_session> write_session;   // Long-lived session
   int                                     root_index = 0;  // Store the assigned root index
   // Add schema info if needed (column names, types) parsed from CREATE VIRTUAL TABLE
   std::vector<std::pair<std::string, int>> column_info;  // Pair of <name, sqlite_type>
};

// Represents a cursor for iterating over the virtual table
struct arbtrie_cursor
{
   sqlite3_vtab_cursor base;   // Base class for virtual table cursors
   arbtrie_vtab*       pVtab;  // Pointer back to the virtual table instance

   // Use optional for the read transaction (iterator)
   std::optional<arbtrie::read_transaction> read_tx;

   // how many rows are left to iterate over
   int rows_remaining = 0;
   int idxNum         = 0;  /// filter index number
};

// --- Virtual Table Method Implementations ---

// Connect/Create a virtual table instance
static int arbtrieConnect(sqlite3*           db,
                          void*              pAux,
                          int                argc,
                          const char* const* argv,
                          sqlite3_vtab**     ppVtab,
                          char**             pzErr)
{
   // Table name is argv[2]
   if (argc < 3)
   {
      *pzErr = sqlite3_mprintf("Internal error: Table name missing");
      return SQLITE_ERROR;
   }
   std::string table_name = argv[2];

   // pAux is client data from sqlite3_create_module - WE WILL NOT USE THIS.
   // Instead, parse argv from CREATE VIRTUAL TABLE statement.
   // Example: CREATE VIRTUAL TABLE x USING arbtrie(path='/path/to/db', key TEXT, ...)

   std::string db_path;
   std::string schema     = "CREATE TABLE x(";  // Start building schema string
   bool        path_found = false;
   bool        first_col  = true;  // Track if we are adding the first column
   std::vector<std::pair<std::string, int>> column_info_temp;  // Temporary store for column info

   // argv[0] = module name ("arbtrie")
   // argv[1] = database name (unused)
   // argv[2] = table name ("kv_text" etc.)
   // argv[3 onwards] = arguments passed in parentheses
   for (int i = 3; i < argc; ++i)
   {
      std::string arg         = argv[i];
      std::string path_prefix = "path=";
      if (arg.rfind(path_prefix, 0) == 0)  // Check if arg starts with "path="
      {
         db_path = arg.substr(path_prefix.length());
         // Trim potential quotes
         if (db_path.length() >= 2 && db_path.front() == '\'' && db_path.back() == '\'')
         {
            db_path = db_path.substr(1, db_path.length() - 2);
         }
         path_found = true;
      }
      else
      {
         // Assume other arguments are column definitions for the schema
         if (!first_col)  // Add comma *before* subsequent columns
         {
            schema += ", ";
         }
         schema += arg;
         first_col = false;  // Mark that we've added at least one column

         // --- Parse column name and type ---
         std::string col_def = arg;
         // Basic parsing: find first space to separate name and type
         size_t space_pos = col_def.find_first_of(" \t");
         if (space_pos != std::string::npos)
         {
            std::string col_name = col_def.substr(0, space_pos);
            // Find the start of the type (skip spaces)
            size_t type_start_pos = col_def.find_first_not_of(" \t", space_pos);
            if (type_start_pos != std::string::npos)
            {
               std::string type_decl = col_def.substr(type_start_pos);
               // Further refine type (e.g., ignore PRIMARY KEY, NOT NULL for type detection)
               size_t      first_word_end = type_decl.find_first_of(" \t(");
               std::string base_type      = (first_word_end == std::string::npos)
                                                ? type_decl
                                                : type_decl.substr(0, first_word_end);
               column_info_temp.push_back({col_name, get_sqlite_type_from_string(base_type)});
               std::cout << "Parsed column: Name='" << col_name << "', Declared='" << type_decl
                         << "', Deduced Type=" << get_sqlite_type_from_string(base_type)
                         << std::endl;  // DEBUG
            }
            else
            {
               std::cerr << "Warning: Could not parse type for column definition: " << arg
                         << std::endl;
               column_info_temp.push_back(
                   {col_name, SQLITE_BLOB});  // Default to BLOB on parse error
            }
         }
         else
         {
            std::cerr << "Warning: Could not parse column definition: " << arg << std::endl;
            column_info_temp.push_back({arg, SQLITE_BLOB});  // Default name and BLOB on parse error
         }
         // --- End Parse column name and type ---
      }
   }
   schema += ") WITHOUT ROWID;";  // Close schema string

   std::cout << "Schema: " << schema << std::endl;

   if (!path_found)
   {
      *pzErr = sqlite3_mprintf(
          "Mandatory 'path' argument not provided in CREATE VIRTUAL TABLE statement (e.g., "
          "path='/path/to/db')");
      return SQLITE_ERROR;
   }

   // Allocate the virtual table structure
   arbtrie_vtab* pNewVtab = new (std::nothrow) arbtrie_vtab();
   if (!pNewVtab)
      return SQLITE_NOMEM;
   std::memset(pNewVtab, 0, sizeof(*pNewVtab));

   pNewVtab->db              = db;
   pNewVtab->arbtrie_db_path = db_path;
   pNewVtab->column_info     = std::move(column_info_temp);  // Store parsed column info

   // --- Create/Open Arbtrie Database and Write Session ---
   // Each virtual table instance gets its own database connection based on path.
   try
   {
      // Check if the database needs physical creation on disk
      if (!std::filesystem::exists(db_path))
      {
         arbtrie::database::create(db_path);
         std::cout << "Created new arbtrie database at: " << db_path << std::endl;
      }

      // Step 1: Create the database object
      pNewVtab->arbtrie_db = std::make_shared<arbtrie::database>(db_path);

      // Step 2: Create the long-lived write session from the database object
      pNewVtab->write_session = pNewVtab->arbtrie_db->start_write_session();

      std::cout << "Opened arbtrie database for vtab at: " << db_path << std::endl;
   }
   catch (const std::exception& e)
   {
      *pzErr = sqlite3_mprintf("Failed to create/open arbtrie database '%s': %s", db_path.c_str(),
                               e.what());
      delete pNewVtab;
      return SQLITE_ERROR;
   }
   // --- End Database Instance Handling ---

   // --- Assign Root Index ---
   // Since each vtab now has its own independent database instance,
   // we can simply use the default root index (0) for all tables.
   pNewVtab->root_index = 0;
   std::cout << "Using default root index 0 for table '" << table_name << "'" << std::endl;
   // --- End Root Index Assignment ---

   // Declare the table schema to SQLite.
   // Use the schema constructed from the arguments.
   // const char* schema = "CREATE TABLE x(key TEXT PRIMARY KEY, value BLOB);";
   // --- DEBUG: Log Schema and Vtab Declaration ---
   std::cout << "Declaring vtab with schema: " << schema << std::endl;
   int rc = sqlite3_declare_vtab(db, schema.c_str());
   std::cout << "sqlite3_declare_vtab returned: " << rc << std::endl;
   if (rc != SQLITE_OK)
   {
      *pzErr = sqlite3_mprintf("Failed to declare vtab schema: %s", sqlite3_errmsg(db));
      std::cerr << "sqlite3_declare_vtab error: " << *pzErr << std::endl;
      // --- END DEBUG ---
      // No need to manually cleanup cache, shared_ptr handles lifetime
      delete pNewVtab;
      return rc;
   }

   *ppVtab = &pNewVtab->base;
   return SQLITE_OK;
}

// Disconnect/Destroy a virtual table instance
static int arbtrieDisconnect(sqlite3_vtab* pVtab)
{
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(pVtab);
   // The write_session shared_ptr (p->write_session) goes out of scope here.
   // If this is the last reference, the write_session and the underlying
   // database object it manages will be cleaned up.
   delete p;  // Delete the vtab struct itself
   return SQLITE_OK;
}

// Open a cursor
static int arbtrieOpen(sqlite3_vtab* pVtab, sqlite3_vtab_cursor** ppCursor)
{
   arbtrie_cursor* pCur = new (std::nothrow) arbtrie_cursor();
   if (!pCur)
      return SQLITE_NOMEM;
   std::memset(pCur, 0, sizeof(*pCur));

   pCur->pVtab          = reinterpret_cast<arbtrie_vtab*>(pVtab);  // Store vtab pointer
   pCur->rows_remaining = 0;

   // Iterator is initialized in xFilter

   *ppCursor = &pCur->base;
   return SQLITE_OK;
}

// Close a cursor
static int arbtrieClose(sqlite3_vtab_cursor* cur)
{
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   // read_tx is a unique_ptr, cleans up automatically
   delete pCur;
   return SQLITE_OK;
}

// Determine the best query plan
static int arbtrieBestIndex(sqlite3_vtab* tab, sqlite3_index_info* pIdxInfo)
{
   // This function tells SQLite how efficiently we can access the data
   // based on the WHERE clause constraints (pIdxInfo->aConstraint).

   // Example: Handle equality constraint on the key column (column 0)
   int keyEqIdx = -1;
   for (int i = 0; i < pIdxInfo->nConstraint; ++i)
   {
      if (pIdxInfo->aConstraint[i].usable &&
          pIdxInfo->aConstraint[i].iColumn == 0 &&  // Assuming column 0 is 'key'
          pIdxInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ)
      {
         keyEqIdx = i;
         break;
      }
      // TODO: Add checks for other constraints like LIKE 'prefix%', >, <, etc.
   }

   if (keyEqIdx != -1)
   {
      // We can handle an equality lookup efficiently using arbtrie::get
      pIdxInfo->aConstraintUsage[keyEqIdx].argvIndex =
          1;  // Tell SQLite we need the value for this constraint in xFilter
      pIdxInfo->estimatedCost = 1.0;  // Very cheap cost for direct lookup
      pIdxInfo->estimatedRows = 1;
      pIdxInfo->idxNum        = 1;  // Use 1 to indicate key equality lookup plan
   }
   else
   {
      // Default: Full table scan if no usable index found
      arbtrie_vtab* pVtab = reinterpret_cast<arbtrie_vtab*>(tab);
      // Start a temporary read transaction to count keys for better estimation
      // Note: This introduces a small overhead for planning full scans.
      try
      {
         auto          read_tx   = pVtab->write_session->start_read_transaction(pVtab->root_index);
         sqlite3_int64 num_rows  = read_tx.count_keys();
         pIdxInfo->estimatedRows = num_rows;
         // Estimate cost as roughly proportional to the number of rows to scan
         pIdxInfo->estimatedCost = static_cast<double>(num_rows);
         if (pIdxInfo->estimatedCost < 1.0)
            pIdxInfo->estimatedCost = 1.0;  // Avoid zero cost
         std::cout << "xBestIndex (Full Scan): Estimated rows = " << num_rows
                   << ", Cost = " << pIdxInfo->estimatedCost << std::endl;  // DEBUG
      }
      catch (const std::exception& e)
      {
         // If counting fails, fallback to default large estimates
         std::cerr << "xBestIndex: Error counting keys for estimation: " << e.what() << std::endl;
         pIdxInfo->estimatedCost = 1000000.0;  // High cost
         pIdxInfo->estimatedRows = 1000000;    // High rows
      }

      pIdxInfo->idxNum = 0;  // Use 0 for full scan plan
   }

   // TODO: Implement logic for other constraints (prefix scan, range scans)
   // Assign different idxNum values for different plans.
   // Calculate estimatedCost and estimatedRows appropriately.

   return SQLITE_OK;
}

// Filter results based on index selection
static int arbtrieFilter(sqlite3_vtab_cursor* cur,
                         int                  idxNum,
                         const char*          idxStr,
                         int                  argc,
                         sqlite3_value**      argv)
{
   arbtrie_cursor* pCur  = reinterpret_cast<arbtrie_cursor*>(cur);
   arbtrie_vtab*   pVtab = pCur->pVtab;

   // Store the index number for use in xNext
   pCur->idxNum = idxNum;

   // Start the read transaction (which is an iterator)
   pCur->read_tx.emplace(pVtab->write_session->start_read_transaction(pVtab->root_index));

   // --- Filtering Logic (idxNum == 1 for equality) ---
   if (idxNum == 1)
   {
      if (argc >= 1)
      {
         const unsigned char* key_text = sqlite3_value_text(argv[0]);
         int                  key_len  = sqlite3_value_bytes(argv[0]);
         /*
         // Use the (const char*, len) overload with a cast
         // Store the key for potential use in xColumn
         pCur->filter_key_eq_data.assign(reinterpret_cast<const char*>(key_text),
                                         reinterpret_cast<const char*>(key_text) + key_len);
         arbtrie::key_view filter_key(pCur->filter_key_eq_data.data(),
                                      pCur->filter_key_eq_data.size());
         */
         arbtrie::key_view filter_key =
             to_key_view(reinterpret_cast<const char*>(key_text), key_len);

         // --- RESTORED LOGIC for idxNum=1: Use get() ---
         std::cout << "arbtrieFilter (idxNum=1): Attempting get() for key=X'"
                   << to_hex_string(filter_key) << "'" << std::endl;  // DEBUG
         pCur->rows_remaining = pCur->read_tx->find(filter_key);
         return SQLITE_OK;
      }
   }
   else
   {  // Full scan plan (idxNum == 0)
      std::cout << "arbtrieFilter (idxNum=0): Starting full scan." << std::endl;  // DEBUG
      pCur->rows_remaining = pCur->read_tx->count_keys();
      if (pCur->rows_remaining > 0)
         pCur->read_tx->begin();
      return SQLITE_OK;
   }
   return SQLITE_OK;
}

// Advance the cursor to the next row of the result set (not the database)
static int arbtrieNext(sqlite3_vtab_cursor* cur)
{
   std::cout << ">>> arbtrieNext called" << std::endl;  // DEBUG
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   assert(pCur->rows_remaining > 0);
   pCur->read_tx->next();
   pCur->rows_remaining--;
   return SQLITE_OK;
}

// Check if the cursor is at the end of the result set
static int arbtrieEof(sqlite3_vtab_cursor* cur)
{
   ARBTRIE_WARN("arbtrieEof called");
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   return pCur->rows_remaining == 0;
}

// Retrieve a column value for the current row
static int arbtrieColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i)
{
   std::cout << ">>> arbtrieColumn called for index " << i << std::endl;  // DEBUG
   arbtrie_cursor* pCur  = reinterpret_cast<arbtrie_cursor*>(cur);
   arbtrie_vtab*   pVtab = pCur->pVtab;  // Get the vtab pointer

   // Check EOF first
   if (pCur->rows_remaining == 0)
   {
      ARBTRIE_ERROR("arbtrieColumn: EOF or invalid transaction");
      sqlite3_result_text(ctx, "reading at eof", -1, SQLITE_STATIC);
      return SQLITE_ERROR;
   }

   // Validate column index
   if (i < 0 || static_cast<size_t>(i) >= pVtab->column_info.size())
   {
      ARBTRIE_ERROR("arbtrieColumn: invalid column index ", i);
      sqlite3_result_error(ctx, "Invalid column index", -1);
      return SQLITE_ERROR;
   }

   int declared_type = pVtab->column_info[i].second;
   std::cout << "    Column index " << i << " declared type: " << declared_type
             << std::endl;  // DEBUG

   // We assume the iterator points to the correct row found by xFilter or advanced by xNext.
   // Handle key and value based on stored column info (assuming key is always column 0, value is 1 for simplicity now)
   // TODO: Make this more robust if column order/meaning can change.

   if (i == 0)  // Assuming key is always column 0
   {
      arbtrie::key_view key = pCur->read_tx->key();
      std::cout << "    arbtrieColumn (key from iterator): size=" << key.size() << ", data='"
                << to_hex_string(key) << "'" << std::endl;  // DEBUG

      // Return key based on its declared type (usually TEXT or BLOB)
      switch (declared_type)
      {
         case SQLITE_TEXT:
            sqlite3_result_text(ctx, key.data(), key.size(), SQLITE_STATIC);
            break;
         case SQLITE_BLOB:
         default:  // Default to BLOB if not TEXT
            sqlite3_result_blob(ctx, key.data(), key.size(), SQLITE_STATIC);
            break;
      }
      return SQLITE_OK;
   }
   else  // Assuming value is any column other than 0 for now
   {
      return pCur->read_tx->value(
          [&](arbtrie::value_type value)
          {
             if (not value.is_view())
             {
                sqlite3_result_text(ctx, "unsupported value type", -1, SQLITE_STATIC);
                ARBTRIE_ERROR("arbtrieColumn: unsupported value type");
                return SQLITE_ERROR;
             }
             auto v = value.view();
             ARBTRIE_INFO("arbtrieColumn: returning value size=", v.size());

             // Return value based on its declared type
             switch (declared_type)
             {
                case SQLITE_TEXT:
                   std::cout << "    Returning column " << i << " as TEXT" << std::endl;  // DEBUG
                   // Using SQLITE_TRANSIENT because the underlying data might change before SQLite copies it.
                   sqlite3_result_text(ctx, v.data(), v.size(), SQLITE_TRANSIENT);
                   break;
                case SQLITE_INTEGER:
                   std::cout << "    Returning column " << i
                             << " as INTEGER (conversion not implemented)" << std::endl;  // DEBUG
                   // TODO: Implement conversion from blob to integer if needed
                   sqlite3_result_error(ctx, "INTEGER conversion not implemented", -1);
                   return SQLITE_ERROR;
                case SQLITE_FLOAT:
                   std::cout << "    Returning column " << i
                             << " as REAL (conversion not implemented)" << std::endl;  // DEBUG
                   // TODO: Implement conversion from blob to float/double if needed
                   sqlite3_result_error(ctx, "REAL conversion not implemented", -1);
                   return SQLITE_ERROR;
                case SQLITE_BLOB:
                default:  // Default to BLOB
                   std::cout << "    Returning column " << i << " as BLOB" << std::endl;  // DEBUG
                   sqlite3_result_blob(ctx, v.data(), v.size(), SQLITE_TRANSIENT);
                   break;
             }
             return SQLITE_OK;
          });
   }
   // Should not be reached if column index is valid
   ARBTRIE_ERROR("arbtrieColumn: reached end unexpectedly for index ", i);
   sqlite3_result_error(ctx, "Internal error processing column", -1);
   return SQLITE_ERROR;
}

// Retrieve the rowid (not typically used for WITHOUT ROWID tables)
static int arbtrieRowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid)
{
   std::cout << ">>> arbtrieRowid called" << std::endl;  // DEBUG
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   *pRowid              = 0;  //pCur->current_rowid;
   return SQLITE_OK;
}

// Handle INSERT, DELETE, UPDATE operations
static int arbtrieUpdate(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite_int64* pRowid)
{
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(pVtab);
   std::cout << ">>> arbtrieUpdate called with argc = " << argc << std::endl;

   // Ensure the write_session is valid
   if (!p->write_session)
   {
      std::cerr << "Error: arbtrieUpdate pVtab has null write_session." << std::endl;
      p->base.zErrMsg = sqlite3_mprintf("Internal virtual table error: write session missing.");
      return SQLITE_ERROR;
   }

   // Get the long-lived write_session
   auto& session = *p->write_session;
   // Start a transaction on the existing session
   arbtrie::write_transaction tx = session.start_transaction(p->root_index);

   // Determine operation type based on argc and argv contents
   // See: https://www.sqlite.org/vtab.html#xupdate
   std::string op_type = "UNKNOWN";  // For logging

   try
   {
      // Determine operation type based on argc and arguments (per sqlite.org/vtab.html#xupdate)
      // Assuming N=2 columns (key, value)
      if (argc == 1)
      {
         // Rowid DELETE - Unsupported
         // --- DEBUG: Log argc=1 argument ---
         int type = sqlite3_value_type(argv[0]);
         std::cout << "arbtrieUpdate (argc=1): argv[0] type=" << type << std::endl;

         if (type == SQLITE_TEXT)
         {
            // Handling DELETE for WITHOUT ROWID tables where key is passed in argv[0]
            op_type = "DELETE (WITHOUT ROWID)";  // Set op_type here
            arbtrie::key_view key_to_delete =
                to_key_view(reinterpret_cast<const char*>(sqlite3_value_text(argv[0])),
                            sqlite3_value_bytes(argv[0]));
            std::string key_str(key_to_delete.data(), key_to_delete.size());
            std::cout << "  Attempting DELETE (WITHOUT ROWID style) for key: '" << key_str << "'"
                      << std::endl;
            int removed = tx.remove(key_to_delete);
            if (removed <= 0)
            {
               std::cerr << "  Warning: Key to delete not found: " << key_str << std::endl;
               // Return OK even if not found, standard SQL DELETE behavior
            }
            else
            {
               std::cout << "  Delete successful (removed=" << removed << ")" << std::endl;
            }
            // rc remains SQLITE_OK
         }
         else if (type == SQLITE_INTEGER)
         {
            // Traditional rowid DELETE - Unsupported
            std::cout << "  Integer value (rowid?) received: " << sqlite3_value_int64(argv[0])
                      << std::endl;
            std::cerr << "  Error: Rowid DELETE (op_type=DELETE, argc=1, argv[0]=INTEGER) not "
                         "supported."
                      << std::endl;
            return SQLITE_CONSTRAINT;
         }
         else
         {
            // NULL or other unexpected type
            std::cout << "  Received NULL or unexpected type for key/rowid." << std::endl;
            std::cerr << "  Error: Invalid argument for DELETE (op_type=DELETE, argc=1)."
                      << std::endl;
            return SQLITE_MISUSE;
         }
      }
      else if (argc == 2 && sqlite3_value_type(argv[1]) == SQLITE_NULL)
      {
         // Key-based DELETE
         std::cout << "arbtrieUpdate (DELETE): argc=2, argv[1]=NULL" << std::endl;
         op_type = "DELETE";
         if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
         {
            std::cerr << "DELETE error: Key (argv[0]) is NULL." << std::endl;
            return SQLITE_MISUSE;  // Cannot delete NULL key
         }
         else
         {
            arbtrie::key_view key_to_delete =
                to_key_view(reinterpret_cast<const char*>(sqlite3_value_text(argv[0])),
                            sqlite3_value_bytes(argv[0]));
            std::cout << "  Attempting to delete key=X'" << to_hex_string(key_to_delete) << "'"
                      << std::endl;
            int removed = tx.remove(key_to_delete);
            if (removed <= 0)
            {
               std::cerr << "  Warning: Key to delete not found: X'" << to_hex_string(key_to_delete)
                         << "'" << std::endl;
               // rc remains SQLITE_OK
            }
            // rc remains SQLITE_OK
            std::cout << "  Delete successful (removed=" << removed << ")" << std::endl;
         }
      }
      else if (argc == 4 && sqlite3_value_type(argv[0]) == SQLITE_NULL)
      {
         // INSERT (argc=2+N=4)
         op_type = "INSERT";
         std::cout << "arbtrieUpdate (INSERT): argc=4, argv[0]=NULL" << std::endl;
         // We already know argc is 4
         // if (argc < 4) return SQLITE_MISUSE;
         // Cast needed for to_key_view overload
         arbtrie::key_view key =
             to_key_view(reinterpret_cast<const char*>(sqlite3_value_text(argv[2])),
                         sqlite3_value_bytes(argv[2]));
         const void*         val_blob = sqlite3_value_blob(argv[3]);
         int                 val_len  = sqlite3_value_bytes(argv[3]);
         arbtrie::value_view value(static_cast<const char*>(val_blob), val_len);
         try
         {
            std::cout << "  Attempting upsert (INSERT) key=X'" << to_hex_string(key)
                      << "', value=X'" << to_hex_string(value) << "'"
                      << std::endl;  // Log already says upsert
                                     // --- Use upsert ---
            tx.upsert(key, value);
         }
         catch (const std::exception& e)
         {
            std::cerr << "Arbtrie insert failed: " << e.what() << std::endl;
            return SQLITE_CONSTRAINT;
         }
      }
      else if (argc == 6)
      {
         // UPDATE (argc=2+2*N=6)
         op_type = "UPDATE";
         // UPDATE requires: argv[0] (module name), argv[1] (unused?),
         // argv[2] (rowid/old key?), argv[3] (unused?), argv[4] (new key), argv[5] (new value)
         // So argc must be at least 6
         // We already know argc is 6
         // if (argc < 6) return SQLITE_MISUSE;

         // --- DEBUG: Log UPDATE arguments ---
         std::cout << "arbtrieUpdate (UPDATE): argc=" << argc << std::endl;
         for (int i = 0; i < argc; ++i)
         {
            int type = sqlite3_value_type(argv[i]);
            std::cout << "  argv[" << i << "] type=" << type;
            if (type == SQLITE_NULL)
            {
               std::cout << " (NULL)" << std::endl;
            }
            else if (type == SQLITE_INTEGER)
            {
               std::cout << " value=" << sqlite3_value_int64(argv[i]) << std::endl;
            }
            else if (type == SQLITE_TEXT)
            {
               std::cout << " len=" << sqlite3_value_bytes(argv[i]) << " value='"
                         << sqlite3_value_text(argv[i]) << "'" << std::endl;
            }
            else if (type == SQLITE_BLOB)
            {
               std::cout << " len=" << sqlite3_value_bytes(argv[i]) << " (BLOB)" << std::endl;
            }
            else
            {
               std::cout << " (Other type)" << std::endl;
            }
         }
         // --- END DEBUG ---

         // Copy data from argv into local storage to ensure lifetime
         const unsigned char* old_key_ptr = sqlite3_value_text(argv[2]);
         int                  old_key_len = sqlite3_value_bytes(argv[2]);
         std::vector<char>    old_key_data(old_key_ptr, old_key_ptr + old_key_len);
         arbtrie::key_view    old_key(old_key_data.data(), old_key_data.size());

         // Indices shift based on understanding: assuming argv[2] is old_key or related ID
         // and argv[4] is new_key, argv[5] is new_value
         const unsigned char* new_key_ptr = sqlite3_value_text(argv[4]);  // Index for new key
         int                  new_key_len = sqlite3_value_bytes(argv[4]);
         std::vector<char>    new_key_data(new_key_ptr, new_key_ptr + new_key_len);
         arbtrie::key_view    new_key(new_key_data.data(), new_key_data.size());

         const void*         new_val_ptr = sqlite3_value_blob(argv[5]);  // Index for new value
         int                 new_val_len = sqlite3_value_bytes(argv[5]);
         std::vector<char>   new_val_data(static_cast<const char*>(new_val_ptr),
                                          static_cast<const char*>(new_val_ptr) + new_val_len);
         arbtrie::value_view new_value(new_val_data.data(), new_val_data.size());

         try
         {
            std::cout << "  Attempting upsert (UPDATE) key=X'" << to_hex_string(old_key)
                      << "', value=X'" << to_hex_string(new_value) << "'"
                      << std::endl;  // Log already says upsert
            try
            {
               // --- Use upsert ---
               tx.upsert(old_key, new_value);
               // tx.remove(old_key); // Remove the old value definitively
               // tx.insert(new_key, new_value); // Insert the new value
            }
            catch (const std::exception& e)
            {
               std::cerr << "  Arbtrie upsert failed (exception): " << e.what() << std::endl;
               return SQLITE_CONSTRAINT;  // Or maybe SQLITE_ERROR
            }
         }
         catch (const std::exception& e)
         {
            std::cerr << "Arbtrie update failed: " << e.what() << std::endl;
            return SQLITE_CONSTRAINT;
         }
      }
      else if (argc == 4)
      {
         // Optimized UPDATE (WITHOUT ROWID, PK not changed)
         // Add detailed logging for argv[0]..argv[2]
         std::cout << "  Raw argv[0] (old key?): type=" << sqlite3_value_type(argv[0])
                   << ", value=X'"
                   << to_hex_string(
                          reinterpret_cast<const unsigned char*>(sqlite3_value_text(argv[0])),
                          sqlite3_value_bytes(argv[0]))
                   << "'" << std::endl;
         std::cout << "  Raw argv[1] (new key?): type=" << sqlite3_value_type(argv[1])
                   << ", value=X'"
                   << to_hex_string(
                          reinterpret_cast<const unsigned char*>(sqlite3_value_text(argv[1])),
                          sqlite3_value_bytes(argv[1]))
                   << "'" << std::endl;
         std::cout << "  Raw argv[2] (value?):   type=" << sqlite3_value_type(argv[2])
                   << ", value=X'"
                   << to_hex_string(
                          reinterpret_cast<const unsigned char*>(sqlite3_value_blob(argv[2])),
                          sqlite3_value_bytes(argv[2]))  // Use blob for value
                   << "'" << std::endl;
         // Log argv[3] as well, as it might contain the value
         if (argc > 3)
         {  // Check if argv[3] exists
            std::cout << "  Raw argv[3] (value?):   type=" << sqlite3_value_type(argv[3])
                      << ", value=X'"
                      << to_hex_string(
                             reinterpret_cast<const unsigned char*>(sqlite3_value_blob(argv[3])),
                             sqlite3_value_bytes(argv[3]))  // Use blob for value
                      << "'" << std::endl;
         }

         op_type = "UPDATE (optimized)";
         std::cout << "arbtrieUpdate (UPDATE optimized): argc=4" << std::endl;

         if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
             sqlite3_value_type(argv[1]) == SQLITE_NULL)
         {
            std::cerr << "  Error: Key is NULL in optimized UPDATE (argc=4)." << std::endl;
            return SQLITE_MISUSE;
         }
         else
         {
            // Key should be in argv[0] (or argv[1], they are the same)
            arbtrie::key_view key =
                to_key_view(reinterpret_cast<const char*>(sqlite3_value_text(argv[0])),
                            sqlite3_value_bytes(argv[0]));
            // Based on logs, value seems to be in argv[3] for this argc=4 case
            const void*         val_blob = sqlite3_value_blob(argv[3]);
            int                 val_len  = sqlite3_value_bytes(argv[3]);
            arbtrie::value_view value(static_cast<const char*>(val_blob), val_len);

            std::cout << "  Attempting upsert (UPDATE) key=X'" << to_hex_string(key)
                      << "', value=X'" << to_hex_string(value) << "'"
                      << std::endl;  // Log already says upsert
            try
            {
               // --- Use upsert ---
               tx.upsert(key, value);
               // tx.remove(key); // Remove the old value definitively
               // tx.insert(key, value); // Insert the new value
            }
            catch (const std::exception& e)
            {
               std::cerr << "  Arbtrie upsert failed (exception): " << e.what() << std::endl;
               return SQLITE_CONSTRAINT;  // Or maybe SQLITE_ERROR
            }
         }
      }
      else
      {
         // Unexpected argument count
         std::cerr << "arbtrieUpdate Error: Unexpected argc=" << argc
                   << " - Cannot determine operation type." << std::endl;
         return SQLITE_MISUSE;
      }

      // Only attempt commit if rc is still SQLITE_OK (i.e., not skipped by diagnostic)
      if (SQLITE_OK == SQLITE_OK)
      {
         try
         {
            std::cout << "arbtrieUpdate: Committing transaction for op_type=" << op_type
                      << std::endl;
            tx.commit();

            return SQLITE_OK;
         }
         catch (const std::exception& e)
         {
            std::cerr << "arbtrieUpdate Error: Arbtrie commit failed for op_type=" << op_type
                      << ": " << e.what() << std::endl;
            return SQLITE_ERROR;
         }
      }
      else
      {
         // Abort via RAII
         std::cerr << "arbtrieUpdate: Aborting transaction (rc != SQLITE_OK) for op_type="
                   << op_type << ", rc=" << SQLITE_OK << std::endl;
         return SQLITE_ERROR;
      }
   }
   catch (const std::exception& e)
   {
      std::cerr << "arbtrieUpdate Error: Exception during update: " << e.what() << std::endl;
      return SQLITE_ERROR;
   }
}

// --- Module Definition ---

// Define the SQLite module structure
// Remove 'static' to allow external linkage as declared in main.cpp
extern "C" const sqlite3_module arbtrieModule = {
    3,                 /* iVersion - Set >=3 to support newer flags if needed */
    arbtrieConnect,    /* xCreate */
    arbtrieConnect,    /* xConnect */
    arbtrieBestIndex,  /* xBestIndex */
    arbtrieDisconnect, /* xDisconnect */
    arbtrieDisconnect, /* xDestroy */
    arbtrieOpen,       /* xOpen */
    arbtrieClose,      /* xClose */
    arbtrieFilter,     /* xFilter */
    arbtrieNext,       /* xNext */
    arbtrieEof,        /* xEof */
    arbtrieColumn,     /* xColumn */
    arbtrieRowid,      /* xRowid */
    arbtrieUpdate,     /* xUpdate */
    nullptr,           /* xBegin */
    nullptr,           /* xSync */
    nullptr,           /* xCommit */
    nullptr,           /* xRollback */
    nullptr,           /* xFindFunction */
    nullptr,           /* xRename */
    nullptr,           /* xSavepoint */
    nullptr,           /* xRelease */
    nullptr,           /* xRollbackTo */
    nullptr            /* xShadowName */
};

// Removed sqlite3_arbtriemodule_init function
// Add the initialization function back for SQLite module registration
extern "C" int sqlite3_arbtriemodule_init(sqlite3*                    db,
                                          char**                      pzErrMsg,
                                          const sqlite3_api_routines* pApi)
{
   SQLITE_EXTENSION_INIT2(pApi);
   // The client data (4th arg) is passed as pAux to xConnect/xCreate.
   // Often NULL, as config (like DB path) comes from CREATE VIRTUAL TABLE args.
   int rc = sqlite3_create_module(db, "arbtrie", &arbtrieModule, nullptr /* pClientData */);
   return rc;
}