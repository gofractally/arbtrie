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
#include <optional>  // Needed for std::optional
#include <sstream>   // Required for std::ostringstream
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

   // --- Transaction Management ---
   // Use a stack to manage nested transactions/savepoints
   std::vector<arbtrie::write_transaction::ptr> tx_stack;

   arbtrie_vtab() { tx_stack.reserve(64); }
   // std::optional<arbtrie::write_transaction> current_tx; // REMOVED
   // int savepoint_depth = 0; // REMOVED
   // We might need a mutex here if multiple connections could share this vtab instance,
   // but assuming standard setup where each connection gets its own for now.
   // std::mutex tx_mutex;
};

// Represents a cursor for iterating over the virtual table
struct arbtrie_cursor
{
   sqlite3_vtab_cursor base;   // Base class for virtual table cursors
   arbtrie_vtab*       pVtab;  // Pointer back to the virtual table instance

   // Use optional for the read transaction (iterator)
   arbtrie::read_transaction::ptr read_tx;

   // how many rows are left to iterate over
   int           rows_remaining = 0;
   int           idxNum         = 0;   /// filter index number
   sqlite3_int64 count_result   = -1;  // Store count for COUNT(*) optimization
};

// --- Forward declarations for Transaction Methods ---
// static int arbtrieSavepoint(sqlite3_vtab* pVtab, int iSavepoint); // REMOVED
static int arbtrieRelease(sqlite3_vtab* pVtab, int iSavepoint);
static int arbtrieRollbackTo(sqlite3_vtab* pVtab, int iSavepoint);

// --- Virtual Table Method Implementations ---

// Connect/Create a virtual table instance
static int arbtrieConnect(sqlite3*           db,
                          void*              pAux,
                          int                argc,
                          const char* const* argv,
                          sqlite3_vtab**     ppVtab,
                          char**             pzErr)
{
   std::cout << ">>> arbtrieConnect called" << std::endl;  // DEBUG ENTRY
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
   std::cout << ">>> arbtrieDisconnect called" << std::endl;  // DEBUG ENTRY
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
   std::cout << ">>> arbtrieOpen called" << std::endl;  // DEBUG ENTRY
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
   std::cout << ">>> arbtrieClose called" << std::endl;  // DEBUG ENTRY
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   // read_tx is a unique_ptr, cleans up automatically
   delete pCur;
   return SQLITE_OK;
}

// Determine the best query plan
static int arbtrieBestIndex(sqlite3_vtab* tab, sqlite3_index_info* pIdxInfo)
{
   std::cout << ">>> arbtrieBestIndex called" << std::endl;  // DEBUG ENTRY
   // This function tells SQLite how efficiently we can access the data
   // based on the WHERE clause constraints (pIdxInfo->aConstraint).

   arbtrie_vtab* pVtab = reinterpret_cast<arbtrie_vtab*>(tab);

   // --- Identify Constraints ---
   int  keyEqIdx              = -1;
   bool has_other_constraints = false;
   for (int i = 0; i < pIdxInfo->nConstraint; ++i)
   {
      if (pIdxInfo->aConstraint[i].usable)
      {
         if (pIdxInfo->aConstraint[i].iColumn == 0 &&  // Assuming column 0 is 'key'
             pIdxInfo->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ)
         {
            keyEqIdx = i;
            // Don't break here yet, check for other constraints too
         }
         else
         {
            // Found some other usable constraint (range, LIKE, etc.)
            has_other_constraints = true;
         }
      }
   }

   // --- Determine Plan ---

   // Plan 1: Equality Lookup (Highest Priority if available)
   if (keyEqIdx != -1)
   {
      pIdxInfo->aConstraintUsage[keyEqIdx].argvIndex = 1;    // Pass Key to xFilter
      pIdxInfo->estimatedCost                        = 1.0;  // Very cheap
      pIdxInfo->estimatedRows                        = 1;    // Expect 1 row
      pIdxInfo->idxNum                               = 1;    // Use 1 for key equality lookup plan
      std::cout << "xBestIndex: Offering Key Equality plan (idxNum=1)" << std::endl;
   }
   // Plan 2: COUNT(*) Optimization (Only if no constraints and no ORDER BY)
   else if (pIdxInfo->nConstraint == 0 && pIdxInfo->nOrderBy == 0)
   {
      // Offer COUNT(*) optimization plan
      pIdxInfo->idxNum          = 2;     // Use 2 for COUNT(*) plan
      pIdxInfo->estimatedCost   = 0.5;   // Very cheap
      pIdxInfo->estimatedRows   = 1;     // SQLite expects count to return a single row result
      pIdxInfo->orderByConsumed = true;  // Count result has no order
      std::cout << "xBestIndex: Offering COUNT(*) optimization plan (idxNum=2)" << std::endl;

      // Attempt to set omit flag - crucial for signaling SQLite not to aggregate
      // Even if no constraint is found, making the attempt might be important.
      bool found_and_omitted = false;
      for (int i = 0; i < pIdxInfo->nConstraint; ++i)
      {  // Loop likely won't run for pure COUNT(*)
         if (pIdxInfo->aConstraint[i].usable)
         {
            pIdxInfo->aConstraintUsage[i].omit = 1;
            found_and_omitted                  = true;
            std::cout << "xBestIndex: Set omit=1 for constraint " << i
                      << " (likely aggregate column)" << std::endl;
            break;
         }
      }
      if (!found_and_omitted)
      {
         std::cout << "xBestIndex Info: Did not find any usable constraint for COUNT(*) "
                   << "to set omit=1. Relying on idxNum=2 and xColumn/rows_remaining logic."
                   << std::endl;
      }
   }
   // Plan 0: Full Scan (Default for everything else)
   else
   {
      // Full table scan if no specific plan matches
      std::cout << "xBestIndex: Falling back to Full Scan plan (idxNum=0)" << std::endl;
      if (has_other_constraints)
      {
         std::cout << "  Reason: Found other usable constraints." << std::endl;
      }
      if (pIdxInfo->nOrderBy > 0)
      {
         std::cout << "  Reason: Query has ORDER BY clause." << std::endl;
      }

      try
      {
         auto          read_tx   = pVtab->write_session->start_read_transaction(pVtab->root_index);
         sqlite3_int64 num_rows  = read_tx->count_keys();
         pIdxInfo->estimatedRows = num_rows;
         pIdxInfo->estimatedCost = static_cast<double>(num_rows);  // Base cost
         // Add cost for sorting if ORDER BY is present but not consumed
         if (pIdxInfo->nOrderBy > 0 && !pIdxInfo->orderByConsumed)
         {
            // Simple estimate: N*logN cost for sorting
            pIdxInfo->estimatedCost += num_rows > 1 ? (num_rows * log(num_rows)) : 0;
            std::cout << "  Adding estimated sort cost." << std::endl;
         }
         if (pIdxInfo->estimatedCost < 1.0)
            pIdxInfo->estimatedCost = 1.0;
         std::cout << "  Estimated rows = " << num_rows
                   << ", Estimated Cost = " << pIdxInfo->estimatedCost << std::endl;
      }
      catch (const std::exception& e)
      {
         std::cerr << "xBestIndex: Error counting keys for full scan estimation: " << e.what()
                   << std::endl;
         pIdxInfo->estimatedCost = 1000000.0;  // High cost fallback
         pIdxInfo->estimatedRows = 1000000;    // High rows fallback
      }
      pIdxInfo->idxNum = 0;  // Use 0 for full scan plan
   }

   return SQLITE_OK;
}

// Filter results based on index selection
static int arbtrieFilter(sqlite3_vtab_cursor* cur,
                         int                  idxNum,
                         const char*          idxStr,
                         int                  argc,
                         sqlite3_value**      argv)
{
   std::cout << ">>> arbtrieFilter called with idxNum = " << idxNum << std::endl;  // DEBUG ENTRY
   arbtrie_cursor* pCur  = reinterpret_cast<arbtrie_cursor*>(cur);
   arbtrie_vtab*   pVtab = pCur->pVtab;

   // Store the index number for use in xNext/xEof/xColumn
   pCur->idxNum = idxNum;

   // Reset count result before potentially setting it
   pCur->count_result = -1;

   // Reset/Clear existing read transaction before potentially starting a new one
   // Note: read_tx is NOT used for idxNum=2 path, but clear it anyway for consistency
   pCur->read_tx.reset();

   // --- Filtering Logic ---

   // Handle COUNT(*) optimization first
   if (idxNum == 2)
   {
      std::cout << "arbtrieFilter (idxNum=2): Executing COUNT(*) optimization."
                << std::endl;  // DEBUG
      try
      {
         // Use a temporary transaction just for counting
         auto count_tx      = pVtab->write_session->start_read_transaction(pVtab->root_index);
         pCur->count_result = count_tx->count_keys();
         // Set rows_remaining to the ACTUAL count.
         // If SQLite uses xColumn, we provide count_result.
         // If SQLite uses its own count, it will call xNext this many times.
         pCur->rows_remaining = (pCur->count_result >= 0) ? pCur->count_result : 0;
         std::cout << "  COUNT(*) result = " << pCur->count_result
                   << ", setting rows_remaining = " << pCur->rows_remaining << std::endl;  // DEBUG
         // No need to call begin() as we are not iterating the database itself
         return SQLITE_OK;
      }
      catch (const std::exception& e)
      {
         std::cerr << "xFilter (COUNT(*)): Error counting keys: " << e.what() << std::endl;
         pCur->rows_remaining = 0;  // Ensure EOF is true on error
         pCur->count_result   = -1;
         return SQLITE_ERROR;
      }
   }

   // For non-COUNT(*) plans, we need a persistent read transaction
   try
   {
      pCur->read_tx = pVtab->write_session->start_read_transaction(pVtab->root_index);
   }
   catch (const std::exception& e)
   {
      std::cerr << "xFilter (idxNum=" << idxNum
                << "): Error starting read transaction: " << e.what() << std::endl;
      return SQLITE_ERROR;
   }

   // Now handle other idxNums that require the read_tx
   if (idxNum == 1)
   {
      if (argc < 1)
      {
         std::cerr << "xFilter Error: idxNum=1 but argc < 1" << std::endl;
         return SQLITE_MISUSE;
      }
      const unsigned char* key_text = sqlite3_value_text(argv[0]);
      if (!key_text)
      {
         std::cerr << "xFilter Error: idxNum=1 key argument is NULL" << std::endl;
         return SQLITE_MISUSE;
      }
      int               key_len    = sqlite3_value_bytes(argv[0]);
      arbtrie::key_view filter_key = to_key_view(reinterpret_cast<const char*>(key_text), key_len);

      std::cout << "arbtrieFilter (idxNum=1): Attempting find() for key=X'"
                << to_hex_string(filter_key) << "'" << std::endl;  // DEBUG
      pCur->rows_remaining = pCur->read_tx->find(filter_key);
      return SQLITE_OK;
   }
   else if (idxNum == 0)
   {                                                                              // Full scan plan
      std::cout << "arbtrieFilter (idxNum=0): Starting full scan." << std::endl;  // DEBUG
      pCur->rows_remaining = pCur->read_tx->count_keys();  // Get initial count for iteration
      if (pCur->rows_remaining > 0)
      {
         try
         {
            pCur->read_tx->begin();  // Position iterator at the start
         }
         catch (const std::exception& e)
         {
            std::cerr << "xFilter (idxNum=0): Error calling begin(): " << e.what() << std::endl;
            pCur->rows_remaining = 0;  // Ensure EOF on error
            return SQLITE_ERROR;
         }
      }
      return SQLITE_OK;
   }
   else
   {
      // Unknown idxNum (shouldn't happen if BestIndex is correct)
      std::cerr << "xFilter Error: Unknown idxNum = " << idxNum << std::endl;
      return SQLITE_ERROR;
   }
}

// Advance the cursor to the next row of the result set (not the database)
static int arbtrieNext(sqlite3_vtab_cursor* cur)
{
   std::cout << ">>> arbtrieNext called" << std::endl;  // DEBUG ENTRY
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);

   // Handle COUNT(*) path: Just decrement the counter
   if (pCur->idxNum == 2)
   {
      if (pCur->rows_remaining > 0)
      {
         pCur->rows_remaining--;
      }
      // No actual database iteration needed
      std::cout << "  arbtrieNext (idxNum=2): Decremented rows_remaining to "
                << pCur->rows_remaining << std::endl;
      return SQLITE_OK;
   }

   // --- Standard iteration path (idxNum 0 or 1) ---

   // If already at EOF, do nothing (robustness)
   if (pCur->rows_remaining <= 0)
   {
      std::cout << "  arbtrieNext (idxNum=" << pCur->idxNum
                << "): Already at EOF (rows_remaining <= 0)." << std::endl;
      return SQLITE_OK;
   }

   // Only advance the underlying iterator if it exists
   if (pCur->read_tx)
   {
      try
      {
         std::cout << "  arbtrieNext (idxNum=" << pCur->idxNum << "): Calling read_tx->next()."
                   << std::endl;
         pCur->read_tx->next();
      }
      catch (const std::exception& e)
      {
         std::cerr << "arbtrieNext: Error calling next(): " << e.what() << std::endl;
         pCur->rows_remaining = 0;  // Force EOF on error
         return SQLITE_ERROR;
      }
   }
   else
   {
      // Should not happen for idxNum 0 or 1 if xFilter worked
      std::cerr << "arbtrieNext Error: read_tx not available for idxNum=" << pCur->idxNum
                << std::endl;
      pCur->rows_remaining = 0;  // Force EOF
      return SQLITE_ERROR;
   }

   // Decrement counter for standard iteration as well
   pCur->rows_remaining--;
   std::cout << "  arbtrieNext (idxNum=" << pCur->idxNum << "): Decremented rows_remaining to "
             << pCur->rows_remaining << std::endl;
   return SQLITE_OK;
}

// Check if the cursor is at the end of the result set
static int arbtrieEof(sqlite3_vtab_cursor* cur)
{
   // NOTE: No entry log here as it's called very frequently and logs internally already
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   bool            eof  = (pCur->rows_remaining <= 0);
   std::cout << "arbtrieEof called, rows_remaining = " << pCur->rows_remaining << ", returning "
             << (eof ? "true" : "false") << std::endl;  // DEBUG
   return eof;
}

// Retrieve a column value for the current row
static int arbtrieColumn(sqlite3_vtab_cursor* cur, sqlite3_context* ctx, int i)
{
   std::cout << ">>> arbtrieColumn called for index " << i
             << std::endl;  // DEBUG ENTRY (already had one)
   arbtrie_cursor* pCur  = reinterpret_cast<arbtrie_cursor*>(cur);
   arbtrie_vtab*   pVtab = pCur->pVtab;  // Get the vtab pointer

   // Handle COUNT(*) optimization case first
   if (pCur->idxNum == 2)
   {
      std::cout << "    arbtrieColumn (idxNum=2): Handling COUNT(*) result." << std::endl;  // DEBUG
      // We don't check rows_remaining here. If SQLite calls xColumn,
      // it expects the single result row. We provide the stored count.
      if (i == 0)
      {  // Assume COUNT(*) result is requested as the first column
         if (pCur->count_result >= 0)
         {
            std::cout << "    arbtrieColumn (idxNum=2): returning count result="
                      << pCur->count_result << std::endl;
            sqlite3_result_int64(ctx, pCur->count_result);
         }
         else
         {
            // Should not happen if xFilter succeeded, but handle defensively
            std::cerr << "    arbtrieColumn (idxNum=2): Error - count_result is negative!"
                      << std::endl;
            sqlite3_result_error(ctx, "COUNT(*) failed", -1);
            return SQLITE_ERROR;
         }
      }
      else
      {
         // COUNT(*) only provides one column (column 0)
         std::cout << "    arbtrieColumn (idxNum=2): Returning NULL for column " << i << std::endl;
         sqlite3_result_null(ctx);
      }
      return SQLITE_OK;
   }

   // --- Standard Column Retrieval (idxNum 0 or 1) ---

   // Validate column index
   if (i < 0 || static_cast<size_t>(i) >= pVtab->column_info.size())
   {
      std::cerr << "arbtrieColumn: invalid column index " << i << std::endl;
      sqlite3_result_error(ctx, "Invalid column index", -1);
      return SQLITE_ERROR;
   }

   int declared_type = pVtab->column_info[i].second;
   std::cout << "    Column index " << i << " ('" << pVtab->column_info[i].first
             << "') declared type: ";  // DEBUG
   switch (declared_type)
   {
      case SQLITE_INTEGER:
         std::cout << "INTEGER";
         break;
      case SQLITE_FLOAT:
         std::cout << "FLOAT";
         break;
      case SQLITE_TEXT:
         std::cout << "TEXT";
         break;
      case SQLITE_BLOB:
         std::cout << "BLOB";
         break;
      case SQLITE_NULL:
         std::cout << "NULL";
         break;
      default:
         std::cout << "UNKNOWN(" << declared_type << ")";
         break;
   }
   std::cout << std::endl;  // DEBUG

   // Ensure the iterator exists before trying to access it
   if (!pCur->read_tx)
   {
      std::cerr << "arbtrieColumn: read_tx not initialized for idxNum=" << pCur->idxNum
                << std::endl;
      sqlite3_result_error(ctx, "Internal cursor error", -1);
      return SQLITE_ERROR;
   }

   // We assume the iterator points to the correct row found by xFilter or advanced by xNext.
   // Handle key and value based on stored column info (assuming key is always column 0)
   if (i == 0)  // Assuming key is always column 0
   {
      arbtrie::key_view key;
      try
      {
         key = pCur->read_tx->key();
      }
      catch (const std::exception& e)
      {
         std::cerr << "arbtrieColumn: Error getting key from iterator: " << e.what() << std::endl;
         sqlite3_result_error(ctx, "Failed to get key", -1);
         return SQLITE_ERROR;
      }
      std::cout << "    arbtrieColumn (key from iterator): size=" << key.size() << ", data='"
                << to_hex_string(key) << "'" << std::endl;  // DEBUG

      // Return key based on its declared type
      switch (declared_type)
      {
         case SQLITE_TEXT:
            std::cout << "    Returning key as TEXT" << std::endl;  // DEBUG
            sqlite3_result_text(ctx, key.data(), key.size(), SQLITE_TRANSIENT);
            break;
         case SQLITE_BLOB:
         default:  // Default to BLOB if not TEXT (or other specific type)
            std::cout << "    Returning key as BLOB" << std::endl;  // DEBUG
            sqlite3_result_blob(ctx, key.data(), key.size(), SQLITE_TRANSIENT);
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
                std::cerr << "arbtrieColumn: value is not a simple view (subtree?), returning NULL"
                          << std::endl;
                sqlite3_result_null(ctx);
                return SQLITE_OK;  // Return NULL successfully
             }
             auto v = value.view();
             std::cout << "arbtrieColumn: returning value size=" << v.size() << std::endl;

             // Return value based on its declared type
             switch (declared_type)
             {
                case SQLITE_TEXT:
                   std::cout << "    Returning column " << i << " as TEXT" << std::endl;  // DEBUG
                   sqlite3_result_text(ctx, v.data(), v.size(), SQLITE_TRANSIENT);
                   break;
                case SQLITE_INTEGER:
                   std::cout << "    Returning column " << i
                             << " as INTEGER (conversion not implemented, returning BLOB)"
                             << std::endl;  // DEBUG
                   sqlite3_result_blob(ctx, v.data(), v.size(),
                                       SQLITE_TRANSIENT);  // Fallback to BLOB
                   break;
                case SQLITE_FLOAT:
                   std::cout << "    Returning column " << i
                             << " as REAL (conversion not implemented, returning BLOB)"
                             << std::endl;  // DEBUG
                   sqlite3_result_blob(ctx, v.data(), v.size(),
                                       SQLITE_TRANSIENT);  // Fallback to BLOB
                   break;
                case SQLITE_BLOB:
                default:  // Default to BLOB
                   std::cout << "    Returning column " << i << " as BLOB" << std::endl;  // DEBUG
                   sqlite3_result_blob(ctx, v.data(), v.size(), SQLITE_TRANSIENT);
                   break;
             }
             return SQLITE_OK;
          });
   }
}

// Retrieve the rowid (not typically used for WITHOUT ROWID tables)
static int arbtrieRowid(sqlite3_vtab_cursor* cur, sqlite_int64* pRowid)
{
   std::cout << ">>> arbtrieRowid called" << std::endl;  // DEBUG ENTRY
   arbtrie_cursor* pCur = reinterpret_cast<arbtrie_cursor*>(cur);
   *pRowid              = 0;  //pCur->current_rowid;
   return SQLITE_OK;
}

// Handle INSERT, DELETE, UPDATE operations
static int arbtrieUpdate(sqlite3_vtab* pVtab, int argc, sqlite3_value** argv, sqlite_int64* pRowid)
{
   std::cout << ">>> arbtrieUpdate called with argc = " << argc << std::endl;  // DEBUG ENTRY
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(pVtab);
   // std::cout << ">>> arbtrieUpdate called with argc = " << argc << std::endl; // Redundant log removed

   // Ensure the write_session is valid
   if (!p->write_session)
   {
      std::cerr << "Error: arbtrieUpdate pVtab has null write_session." << std::endl;
      p->base.zErrMsg = sqlite3_mprintf("Internal virtual table error: write session missing.");
      return SQLITE_ERROR;
   }

   // Determine if we are in an explicit transaction by checking the stack
   bool is_explicit_tx = !p->tx_stack.empty();
   std::cout << "  arbtrieUpdate: Explicit transaction active? "
             << (is_explicit_tx ? "Yes (stack size = " + std::to_string(p->tx_stack.size()) + ")"
                                : "No (Autocommit)")
             << std::endl;

   // Lambda to perform the actual database modification using a given transaction
   auto perform_operation = [&](const arbtrie::write_transaction::ptr& tx) -> int
   {
      std::string op_type = "UNKNOWN";  // Added back for logging in catch block
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
               int removed = tx->remove(key_to_delete);
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
               int removed = tx->remove(key_to_delete);
               if (removed <= 0)
               {
                  std::cerr << "  Warning: Key to delete not found: X'"
                            << to_hex_string(key_to_delete) << "'" << std::endl;
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
               tx->insert(key, value);
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
                  tx->update(old_key, new_value);
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
                  tx->update(key, value);
                  // tx->remove(key); // Remove the old value definitively
                  // tx->insert(key, value); // Insert the new value
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

         // If we reached here, the operation inside the try block succeeded (or was a no-op delete)
         return SQLITE_OK;
      }
      catch (const std::exception& e)
      {
         // Catch exceptions from within the operation logic (e.g., parsing keys/values)
         std::cerr << "arbtrieUpdate Error: Exception during update operation (" << op_type
                   << "): " << e.what() << std::endl;
         p->base.zErrMsg = sqlite3_mprintf("Update operation failed: %s", e.what());
         return SQLITE_ERROR;  // Return specific error code?
      }
   };

   // Execute the operation using the appropriate transaction
   if (is_explicit_tx)
   {
      // Use the transaction at the top of the stack
      return perform_operation(p->tx_stack.back());
      // DO NOT commit or abort here - transaction hooks will handle it.
   }
   else
   {  // Autocommit mode
      // ... (existing autocommit logic using a temporary transaction) ...
      auto temp_tx = p->write_session->start_write_transaction(p->root_index);
      try
      {
         int rc = perform_operation(temp_tx);
         if (rc == SQLITE_OK)
         {
            std::cout << "  arbtrieUpdate (Autocommit): Committing temporary transaction."
                      << std::endl;
            temp_tx->commit();
            return SQLITE_OK;
         }
         else
         {
            // Operation failed, result already set by perform_operation
            std::cout << "  arbtrieUpdate (Autocommit): Operation failed (rc=" << rc
                      << "), aborting." << std::endl;
            temp_tx->abort();  // Ensure abort on failure
            return rc;
         }
      }
      catch (const std::exception& e)
      {
         // Catch exceptions from the temporary transaction commit/abort itself
         std::cerr << "arbtrieUpdate Error (Autocommit): Exception during commit/abort: "
                   << e.what() << std::endl;
         p->base.zErrMsg = sqlite3_mprintf("Autocommit failed: %s", e.what());
         try
         {
            temp_tx->abort();
         }
         catch (...)
         {
         }  // Ensure abort on exception
         return SQLITE_ERROR;
      }
   }
}

// --- Transaction Method Implementations ---

// Handle BEGIN, COMMIT, ROLLBACK, SAVEPOINT (Core Logic) - REMOVED
// static int arbtrieSavepoint(sqlite3_vtab* tab, int op) { /* ... */ }

// --- Delegating Transaction Methods for SQLite Module ---

// xBegin: Called for BEGIN TRANSACTION or SAVEPOINT name
static int arbtrieBegin(sqlite3_vtab* tab)
{
   std::cout << ">>> arbtrieBegin called" << std::endl;  // DEBUG ENTRY
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(tab);
   try
   {
      if (p->tx_stack.empty())
      {
         std::cout << "  arbtrieBegin: Starting base transaction." << std::endl;
         p->tx_stack.emplace_back(p->write_session->start_write_transaction());
      }
      else
      {
         std::cout << "  arbtrieBegin: Starting nested transaction (stack size = "
                   << p->tx_stack.size() << ")." << std::endl;
         // Ensure the back transaction exists before calling start_transaction
         if (!p->tx_stack.empty())
         {
            p->tx_stack.emplace_back(p->tx_stack.back()->start_transaction());
         }
         else
         {
            // This case should logically not be reached due to the outer empty check,
            // but handle defensively.
            std::cerr << "  arbtrieBegin Error: tx_stack became empty unexpectedly." << std::endl;
            return SQLITE_ERROR;
         }
      }
      return SQLITE_OK;
   }
   catch (const std::exception& e)
   {
      std::cerr << "arbtrieBegin Error: Failed to start transaction: " << e.what() << std::endl;
      // Attempt to clean up stack if partially modified
      if (!p->tx_stack.empty())
         p->tx_stack.pop_back();
      return SQLITE_ERROR;
   }
}

// xCommit: Called for COMMIT
static int arbtrieCommit(sqlite3_vtab* tab)
{
   std::cout << ">>> arbtrieCommit called" << std::endl;  // DEBUG ENTRY
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(tab);

   if (p->tx_stack.empty())
   {
      std::cerr << "arbtrieCommit Error: Commit called with no active transaction." << std::endl;
      return SQLITE_ERROR;  // Cannot commit if no transaction exists
   }

   // COMMIT should only apply to the base transaction (stack size 1)
   // If the stack is deeper, it implies unreleased savepoints, which SQLite should handle via xRelease first.
   if (p->tx_stack.size() > 1)
   {
      std::cerr << "arbtrieCommit Error: Commit called with active savepoints (stack size > 1). "
                << "SQLite should have called xRelease first." << std::endl;
      // Optionally, we could force commit all levels, but let's be strict for now.
      return SQLITE_ERROR;
   }

   try
   {
      std::cout << "  arbtrieCommit: Committing base transaction." << std::endl;
      p->tx_stack.back()->commit();
      p->tx_stack.pop_back();  // Remove the committed transaction from the stack
      return SQLITE_OK;
   }
   catch (const std::exception& e)
   {
      std::cerr << "arbtrieCommit Error: Failed to commit transaction: " << e.what() << std::endl;
      // Attempt to abort on commit failure
      try
      {
         if (!p->tx_stack.empty())
            p->tx_stack.back()->abort();
      }
      catch (...)
      {
      }
      p->tx_stack.clear();  // Clear stack on error
      return SQLITE_ERROR;
   }
}

// xRollback: Called for ROLLBACK
static int arbtrieRollback(sqlite3_vtab* tab)
{
   std::cout << ">>> arbtrieRollback called" << std::endl;  // DEBUG ENTRY
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(tab);

   if (p->tx_stack.empty())
   {
      std::cout << "  arbtrieRollback: No active transaction to rollback (no-op)." << std::endl;
      return SQLITE_OK;  // No active transaction, nothing to roll back
   }

   try
   {
      std::cout << "  arbtrieRollback: Aborting all transactions (stack size = "
                << p->tx_stack.size() << ")." << std::endl;
      // Abort transactions from top to bottom
      while (!p->tx_stack.empty())
      {
         p->tx_stack.pop_back();
      }
      std::cout << "  arbtrieRollback: Stack cleared." << std::endl;
      return SQLITE_OK;
   }
   catch (const std::exception& e)
   {
      // Catch potential errors not related to individual aborts (e.g., during stack access)
      std::cerr << "arbtrieRollback Error: General error during rollback: " << e.what()
                << std::endl;
      p->tx_stack.clear();  // Ensure stack is cleared on general error
      return SQLITE_ERROR;
   }
}

// Handle RELEASE savepoint_name
static int arbtrieRelease(sqlite3_vtab* tab, int iSavepoint)
{
   std::cout << ">>> arbtrieRelease called for savepoint index " << iSavepoint << std::endl;
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(tab);

   // SQLite's iSavepoint is 0-based index of *savepoints*, not the stack index.
   // iSavepoint=0 corresponds to tx_stack[1].
   // Releasing savepoint 'iSavepoint' means committing changes from stack indices > iSavepoint + 1
   // down into index iSavepoint + 1, and then resizing the stack.
   int target_stack_index = iSavepoint + 1;
   int current_stack_size = p->tx_stack.size();

   // Basic validation
   if (iSavepoint < 0)
   {
      std::cerr << "arbtrieRelease Error: Negative savepoint index (" << iSavepoint << ")."
                << std::endl;
      return SQLITE_ERROR;
   }
   if (target_stack_index >= current_stack_size)
   {
      // This means trying to release a savepoint that doesn't exist or the base transaction.
      std::cerr << "arbtrieRelease Error: Invalid savepoint index (" << iSavepoint
                << ") for current stack size (" << current_stack_size << ")." << std::endl;
      return SQLITE_ERROR;  // Or maybe SQLITE_OK if SQLite guarantees valid indices?
                            // Let's be strict for now.
   }

   try
   {
      std::cout << "  arbtrieRelease: Releasing transactions from stack index "
                << current_stack_size - 1 << " down to (but not including) " << target_stack_index
                << std::endl;

      // Commit transactions from the top down to the target index.
      // Arbtrie's commit_and_continue merges changes into the parent.
      for (int i = current_stack_size - 1; i >= target_stack_index; --i)
      {
         try
         {
            std::cout << "    Committing stack level " << i << " size: " << p->tx_stack.size()
                      << std::endl;
            // We need commit_and_continue to merge changes down.
            // The last one (at target_stack_index) should just commit normally into its parent?
            // Let's assume commit_and_continue works correctly even for the last one.
            if (i > 0)
            {                             // Need a parent to commit into
               p->tx_stack[i]->commit();  // CORRECT: Merges changes into parent.
               p->tx_stack.pop_back();
            }
            else
            {
               // This case should not be reached due to validation iSavepoint >= 0
               std::cerr << "    Internal Error: Trying to commit_and_continue base transaction?"
                         << std::endl;
               // Should we call commit() here? Unclear. Let's return error for now.
               return SQLITE_ERROR;
            }
         }
         catch (const std::exception& commit_e)
         {
            std::cerr << "    Error committing transaction at stack level " << i << ": "
                      << commit_e.what() << std::endl;
            // If commit fails, we should probably attempt to rollback everything above and including this level?
            // For now, just return the error.
            return SQLITE_ERROR;
         }
      }

      // Resize the stack to keep only the transactions up to and including the target index
      // p->tx_stack.resize(target_stack_index); // Cannot use resize due to missing default ctor
      while (p->tx_stack.size() > target_stack_index)
      {
         p->tx_stack.pop_back();
      }
      std::cout << "  arbtrieRelease: Stack size after pop_back loop = " << p->tx_stack.size()
                << std::endl;
      return SQLITE_OK;
   }
   catch (const std::exception& e)
   {
      std::cerr << "arbtrieRelease Error: General error during release: " << e.what() << std::endl;
      // Attempt to rollback everything on error? Difficult state to recover from.
      return SQLITE_ERROR;
   }
}

// Handle ROLLBACK TO savepoint_name
static int arbtrieRollbackTo(sqlite3_vtab* tab, int iSavepoint)
{
   std::cout << ">>> arbtrieRollbackTo called for savepoint index " << iSavepoint << std::endl;
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(tab);

   // Similar indexing logic as arbtrieRelease
   int target_stack_index = iSavepoint + 1;
   int current_stack_size = p->tx_stack.size();

   // Basic validation
   if (iSavepoint < 0)
   {
      std::cerr << "arbtrieRollbackTo Error: Negative savepoint index (" << iSavepoint << ")."
                << std::endl;
      return SQLITE_ERROR;
   }
   if (target_stack_index >= current_stack_size)
   {
      std::cerr << "arbtrieRollbackTo Error: Invalid savepoint index (" << iSavepoint
                << ") for current stack size (" << current_stack_size << ")." << std::endl;
      return SQLITE_ERROR;
   }

   try
   {
      std::cout << "  arbtrieRollbackTo: Aborting transactions from stack index "
                << current_stack_size - 1 << " down to (but not including) " << target_stack_index
                << std::endl;

      // Abort transactions from the top down to the target index.
      for (int i = current_stack_size - 1; i >= target_stack_index; --i)
      {
         try
         {
            std::cout << "    Aborting stack level " << i << std::endl;
            p->tx_stack[i]->abort();
         }
         catch (const std::exception& abort_e)
         {
            // Log error during individual abort but continue cleaning stack
            std::cerr << "    Error aborting transaction at stack level " << i << ": "
                      << abort_e.what() << std::endl;
            // Even if one abort fails, we should continue trying to abort others and resize.
         }
      }

      // Resize the stack to discard the aborted transactions
      // p->tx_stack.resize(target_stack_index);
      while (p->tx_stack.size() > target_stack_index)
      {
         p->tx_stack.pop_back();
      }
      std::cout << "  arbtrieRollbackTo: Stack size after pop_back loop = " << p->tx_stack.size()
                << std::endl;
      return SQLITE_OK;
   }
   catch (const std::exception& e)
   {
      std::cerr << "arbtrieRollbackTo Error: General error during rollback-to: " << e.what()
                << std::endl;
      // Attempt to rollback everything? Clear stack?
      // Let's try to clear the stack safely using pop_back before returning error
      std::cout << "  Attempting to clear stack fully after general error..." << std::endl;
      while (!p->tx_stack.empty())
      {
         try
         {
            p->tx_stack.back()->abort();  // Try to abort if possible
         }
         catch (...)
         {
         }
         p->tx_stack.pop_back();
      }
      std::cout << "  Stack cleared after general error." << std::endl;
      // p->tx_stack.resize(target_stack_index); // REMOVED - Incorrect resize call
      return SQLITE_ERROR;
   }
}

// Handle SAVEPOINT name (Only called for SAVEPOINT itself when xRelease/xRollbackTo exist)
static int arbtrieSavepoint(sqlite3_vtab* tab, int op)
{
   std::cout << ">>> arbtrieSavepoint called (op ignored: " << op << ")" << std::endl;  // Log entry
   arbtrie_vtab* p = reinterpret_cast<arbtrie_vtab*>(tab);

   // This should only be called for SAVEPOINT name when xRelease/xRollbackTo are defined.
   // We must be inside an existing transaction to create a savepoint.
   if (p->tx_stack.empty())
   {
      std::cerr << "arbtrieSavepoint Error: SAVEPOINT called with no active base transaction."
                << std::endl;
      return SQLITE_ERROR;
   }

   try
   {
      std::cout << "  arbtrieSavepoint: Starting nested transaction (stack size = "
                << p->tx_stack.size() << ")." << std::endl;
      p->tx_stack.emplace_back(p->tx_stack.back()->start_transaction());
      std::cout << "  arbtrieSavepoint: Nested transaction started. New stack size = "
                << p->tx_stack.size() << std::endl;
      return SQLITE_OK;
   }
   catch (const std::exception& e)
   {
      std::cerr << "arbtrieSavepoint Error: Failed to start nested transaction: " << e.what()
                << std::endl;
      // Don't modify stack on error, let SQLite handle cleanup potentially?
      return SQLITE_ERROR;
   }
}

// --- SQLite Module Definition ---
// Rename back to arbtrieModule to match main.cpp reference
// Add back extern "C" for correct linkage
extern "C" const sqlite3_module arbtrieModule = {
    3,               // iVersion = 3 for transaction support
    arbtrieConnect,  // xCreate
    arbtrieConnect,  // xConnect
    arbtrieBestIndex,
    arbtrieDisconnect,  // xDisconnect
    arbtrieDisconnect,  // xDestroy
    arbtrieOpen, arbtrieClose, arbtrieFilter, arbtrieNext, arbtrieEof, arbtrieColumn, arbtrieRowid,
    arbtrieUpdate,
    arbtrieBegin,     // xBegin - Now uses stack
    nullptr,          // xSync
    arbtrieCommit,    // xCommit - Now uses stack
    arbtrieRollback,  // xRollback - Now uses stack
    nullptr,          // xFindFunction
    nullptr,          // xRename
    /* The methods above are in version 1 or 2 */
    arbtrieSavepoint,  // xSavepoint - Added back for SAVEPOINT name
    arbtrieRelease,    // xRelease (needs stack implementation)
    arbtrieRollbackTo  // xRollbackTo (needs stack implementation)
                       /* The methods above are in version 3 */
};

// Add extern "C" back for proper SQLite extension loading
extern "C" int sqlite3_arbtriemodule_init(sqlite3*                    db,
                                          char**                      pzErrMsg,
                                          const sqlite3_api_routines* pApi)
{
   SQLITE_EXTENSION_INIT2(pApi);
   // The client data (4th arg) is passed as pAux to xConnect/xCreate.
   // Often NULL, as config (like DB path) comes from CREATE VIRTUAL TABLE args.
   // Use the corrected module name 'arbtrieModule'
   int rc = sqlite3_create_module(db, "arbtrie", &arbtrieModule, nullptr /* pClientData */);
   return rc;
}