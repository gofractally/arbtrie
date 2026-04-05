#include <psitri-sql/psitri_scanner.hpp>
#include <psitri-sql/psitri_catalog.hpp>
#include <psitri-sql/psitri_transaction.hpp>

#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

#include <psitri/dwal/merge_cursor.hpp>
#include <psitri/dwal/dwal_database.hpp>
#include <psitri-sql/psitri_transaction.hpp>

namespace psitri_sql {

// ===========================================================================
// Bind data
// ===========================================================================

duckdb::unique_ptr<duckdb::FunctionData> PsitriScanBindData::Copy() const {
   auto result     = duckdb::make_uniq<PsitriScanBindData>();
   result->catalog = catalog;
   result->meta    = meta;
   result->has_pk_eq_filter    = has_pk_eq_filter;
   result->pk_eq_key           = pk_eq_key;
   result->has_pk_prefix_filter = has_pk_prefix_filter;
   result->pk_prefix_key       = pk_prefix_key;
   result->has_lower_bound     = has_lower_bound;
   result->has_upper_bound     = has_upper_bound;
   result->lower_bound_key     = lower_bound_key;
   result->upper_bound_key     = upper_bound_key;
   result->has_index_lookup    = has_index_lookup;
   result->index_root          = index_root;
   result->index_lookup_key    = index_lookup_key;
   return result;
}

bool PsitriScanBindData::Equals(const duckdb::FunctionData& other_p) const {
   auto& other = other_p.Cast<PsitriScanBindData>();
   return meta.root_index == other.meta.root_index &&
          meta.table_name == other.meta.table_name;
}

// ===========================================================================
// Helper: convert a DuckDB Value to a ColumnValue for key encoding
// ===========================================================================
static ColumnValue duckdb_value_to_column(const duckdb::Value& val, SqlType type) {
   if (val.IsNull()) return ColumnValue::null_value(type);
   switch (type) {
      case SqlType::BOOLEAN:      return ColumnValue::make_bool(val.GetValue<bool>());
      case SqlType::TINYINT:      return ColumnValue::make_int(SqlType::TINYINT, val.GetValue<int8_t>());
      case SqlType::SMALLINT:     return ColumnValue::make_int(SqlType::SMALLINT, val.GetValue<int16_t>());
      case SqlType::INTEGER:      return ColumnValue::make_int(SqlType::INTEGER, val.GetValue<int32_t>());
      case SqlType::BIGINT:       return ColumnValue::make_int(SqlType::BIGINT, val.GetValue<int64_t>());
      case SqlType::FLOAT:        return ColumnValue::make_float(val.GetValue<float>());
      case SqlType::DOUBLE:       return ColumnValue::make_double(val.GetValue<double>());
      case SqlType::VARCHAR:
      case SqlType::BLOB:         return ColumnValue::make_varchar(val.GetValue<std::string>());
      case SqlType::UTINYINT:     return ColumnValue::make_uint(SqlType::UTINYINT, val.GetValue<uint8_t>());
      case SqlType::USMALLINT:    return ColumnValue::make_uint(SqlType::USMALLINT, val.GetValue<uint16_t>());
      case SqlType::UINTEGER:     return ColumnValue::make_uint(SqlType::UINTEGER, val.GetValue<uint32_t>());
      case SqlType::UBIGINT:      return ColumnValue::make_uint(SqlType::UBIGINT, val.GetValue<uint64_t>());
      case SqlType::DATE:         return ColumnValue::make_date(val.GetValue<int32_t>());
      case SqlType::TIME:         return ColumnValue::make_time(val.GetValue<int64_t>());
      case SqlType::TIMESTAMP:    return ColumnValue::make_timestamp(val.GetValue<int64_t>());
      case SqlType::TIMESTAMP_TZ: return ColumnValue::make_timestamp_tz(val.GetValue<int64_t>());
      case SqlType::HUGEINT: {
         auto h = val.GetValue<duckdb::hugeint_t>();
         return ColumnValue::make_hugeint(h.upper, h.lower);
      }
      case SqlType::UHUGEINT: {
         auto h = val.GetValue<duckdb::uhugeint_t>();
         return ColumnValue::make_uhugeint(h.upper, h.lower);
      }
      case SqlType::UUID: {
         auto h = val.GetValue<duckdb::hugeint_t>();
         return ColumnValue::make_uuid(h.upper, h.lower);
      }
      case SqlType::INTERVAL: {
         auto iv = val.GetValue<duckdb::interval_t>();
         return ColumnValue::make_interval(iv.months, iv.days, iv.micros);
      }
   }
   return ColumnValue::null_value(type);
}

// ===========================================================================
// Init functions
// ===========================================================================

duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
PsitriScanInitGlobal(duckdb::ClientContext& context,
                     duckdb::TableFunctionInitInput& input) {
   auto state        = duckdb::make_uniq<PsitriScanGlobalState>();
   auto& bind_data   = input.bind_data->Cast<PsitriScanBindData>();
   state->root_index = bind_data.meta.root_index;
   return state;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState>
PsitriScanInitLocal(duckdb::ExecutionContext& context,
                    duckdb::TableFunctionInitInput& input,
                    duckdb::GlobalTableFunctionState* global_state) {
   auto state       = duckdb::make_uniq<PsitriScanLocalState>();
   state->column_ids = input.column_ids;

   // Copy PK filter info from bind data (set by pushdown_complex_filter)
   auto& bind_data = input.bind_data->Cast<PsitriScanBindData>();
   state->has_pk_eq_filter    = bind_data.has_pk_eq_filter;
   state->pk_eq_key           = bind_data.pk_eq_key;
   state->has_pk_prefix_filter = bind_data.has_pk_prefix_filter;
   state->pk_prefix_key       = bind_data.pk_prefix_key;
   state->has_lower_bound     = bind_data.has_lower_bound;
   state->has_upper_bound     = bind_data.has_upper_bound;
   state->lower_bound_key     = bind_data.lower_bound_key;
   state->upper_bound_key     = bind_data.upper_bound_key;
   state->has_index_lookup    = bind_data.has_index_lookup;
   state->index_root          = bind_data.index_root;
   state->index_lookup_key    = bind_data.index_lookup_key;

   return state;
}

// ===========================================================================
// Helper: write a decoded ColumnValue into a DuckDB Vector at a given index
// ===========================================================================
static void write_column_to_vector(duckdb::Vector& vec, duckdb::idx_t row,
                                   const ColumnValue& val) {
   if (val.is_null) {
      duckdb::FlatVector::SetNull(vec, row, true);
      return;
   }
   switch (val.type) {
      case SqlType::BOOLEAN:
         duckdb::FlatVector::GetData<bool>(vec)[row] = (val.i64 != 0);
         break;
      case SqlType::TINYINT:
         duckdb::FlatVector::GetData<int8_t>(vec)[row] = static_cast<int8_t>(val.i64);
         break;
      case SqlType::SMALLINT:
         duckdb::FlatVector::GetData<int16_t>(vec)[row] = static_cast<int16_t>(val.i64);
         break;
      case SqlType::INTEGER:
      case SqlType::DATE:
         duckdb::FlatVector::GetData<int32_t>(vec)[row] = static_cast<int32_t>(val.i64);
         break;
      case SqlType::BIGINT:
      case SqlType::TIME:
      case SqlType::TIMESTAMP:
      case SqlType::TIMESTAMP_TZ:
         duckdb::FlatVector::GetData<int64_t>(vec)[row] = val.i64;
         break;
      case SqlType::UTINYINT:
         duckdb::FlatVector::GetData<uint8_t>(vec)[row] = static_cast<uint8_t>(val.i64);
         break;
      case SqlType::USMALLINT:
         duckdb::FlatVector::GetData<uint16_t>(vec)[row] = static_cast<uint16_t>(val.i64);
         break;
      case SqlType::UINTEGER:
         duckdb::FlatVector::GetData<uint32_t>(vec)[row] = static_cast<uint32_t>(val.i64);
         break;
      case SqlType::UBIGINT:
         duckdb::FlatVector::GetData<uint64_t>(vec)[row] = static_cast<uint64_t>(val.i64);
         break;
      case SqlType::FLOAT:
         duckdb::FlatVector::GetData<float>(vec)[row] = static_cast<float>(val.f64);
         break;
      case SqlType::DOUBLE:
         duckdb::FlatVector::GetData<double>(vec)[row] = val.f64;
         break;
      case SqlType::VARCHAR:
      case SqlType::BLOB:
         duckdb::FlatVector::GetData<duckdb::string_t>(vec)[row] =
            duckdb::StringVector::AddString(vec, val.str);
         break;
      case SqlType::HUGEINT:
      case SqlType::UUID: {
         duckdb::hugeint_t h;
         h.upper = val.i64;
         h.lower = val.u64_low;
         duckdb::FlatVector::GetData<duckdb::hugeint_t>(vec)[row] = h;
         break;
      }
      case SqlType::UHUGEINT: {
         duckdb::uhugeint_t h;
         h.upper = static_cast<uint64_t>(val.i64);
         h.lower = val.u64_low;
         duckdb::FlatVector::GetData<duckdb::uhugeint_t>(vec)[row] = h;
         break;
      }
      case SqlType::INTERVAL: {
         duckdb::interval_t iv;
         iv.months = val.interval_months;
         iv.days   = val.interval_days;
         iv.micros = val.interval_micros;
         duckdb::FlatVector::GetData<duckdb::interval_t>(vec)[row] = iv;
         break;
      }
   }
}

// ===========================================================================
// The scan function
// ===========================================================================

void PsitriScanFunction(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output) {
   auto& bind_data = data.bind_data->Cast<PsitriScanBindData>();
   auto& gstate    = data.global_state->Cast<PsitriScanGlobalState>();
   auto& lstate    = data.local_state->Cast<PsitriScanLocalState>();

   if (gstate.finished) {
      output.SetCardinality(0);
      return;
   }

   auto& meta = bind_data.meta;
   auto pk_indices    = meta.pk_column_indices();
   auto val_indices   = meta.value_column_indices();
   auto pk_types      = meta.pk_types();
   auto val_types     = meta.value_types();

   struct ColLocation {
      bool     in_pk;
      uint32_t pos;
   };
   std::vector<ColLocation> col_map(meta.columns.size());
   {
      uint32_t pk_pos = 0, val_pos = 0;
      for (uint32_t i = 0; i < meta.columns.size(); i++) {
         if (meta.columns[i].is_primary_key) {
            col_map[i] = {true, pk_pos++};
         } else {
            col_map[i] = {false, val_pos++};
         }
      }
   }

   // Get the transaction for row_id key registration (needed by DELETE/UPDATE)
   auto& txn = PsitriTransaction::Get(context, *bind_data.catalog);

   // Initialize merge cursor on first call (sees DWAL RW + RO + Tri layers)
   if (!lstate.initialized) {
      txn.ClearRowKeys(gstate.root_index);
      auto& dwal_db = *bind_data.catalog->GetDwalDb();

      // Resolve secondary index lookup into a PK equality filter
      if (lstate.has_index_lookup && !lstate.has_pk_eq_filter) {
         auto idx_result = dwal_db.get_latest(lstate.index_root, lstate.index_lookup_key);
         if (idx_result.found && idx_result.value.is_data() &&
             idx_result.value.data.size() > 0) {
            // Index value is the encoded PK — use it for a point lookup
            lstate.has_pk_eq_filter = true;
            lstate.pk_eq_key = std::string(idx_result.value.data);
         } else {
            // Key not found in index — no rows to return
            gstate.finished = true;
            lstate.initialized = true;
            output.SetCardinality(0);
            return;
         }
      }

      lstate.merge_cursor.emplace(
         dwal_db.create_cursor(gstate.root_index, psitri::dwal::read_mode::latest));

      auto& mc = lstate.merge_cursor->cursor();
      // Workaround: merge cursor lower_bound requires seek_begin first
      // after construction to initialize internal state.
      mc.seek_begin();
      if (lstate.has_pk_eq_filter) {
         mc.lower_bound(lstate.pk_eq_key);
      } else if (lstate.has_pk_prefix_filter) {
         mc.lower_bound(lstate.pk_prefix_key);
      } else if (lstate.has_lower_bound) {
         mc.lower_bound(lstate.lower_bound_key);
      }
      lstate.initialized = true;
   }
   auto& mc = lstate.merge_cursor->cursor();

   duckdb::idx_t count     = 0;
   duckdb::idx_t max_count = STANDARD_VECTOR_SIZE;

   while (count < max_count && !mc.is_end()) {
      auto key_view = mc.key();

      // For PK equality filter: stop after the first non-matching key
      if (lstate.has_pk_eq_filter) {
         if (key_view != lstate.pk_eq_key) {
            gstate.finished = true;
            break;
         }
      }

      // For PK prefix filter: stop when key no longer starts with prefix
      if (lstate.has_pk_prefix_filter) {
         if (key_view.size() < lstate.pk_prefix_key.size() ||
             key_view.substr(0, lstate.pk_prefix_key.size()) != lstate.pk_prefix_key) {
            gstate.finished = true;
            break;
         }
      }

      // For upper bound filter: stop when key exceeds bound
      if (lstate.has_upper_bound) {
         if (key_view > lstate.upper_bound_key) {
            gstate.finished = true;
            break;
         }
      }

      auto pk_vals = decode_key(key_view, pk_types);

      // Read value based on which layer the merge cursor is positioned at
      std::vector<ColumnValue> val_vals;
      auto src = mc.current_source();
      if (src == psitri::dwal::merge_cursor::source::rw ||
          src == psitri::dwal::merge_cursor::source::ro) {
         // DWAL btree layer: value data is in the btree_value
         auto data = mc.current_value().data;
         if (data.size() > 0) {
            val_vals = decode_value(data, val_types);
         } else {
            for (auto t : val_types) {
               val_vals.push_back(ColumnValue::null_value(t));
            }
         }
      } else {
         // Tri layer: use tri cursor's zero-copy get_value callback
         auto* tri = mc.tri_cursor();
         if (tri) {
            tri->get_value([&](psitri::value_view vv) {
               if (vv.size() > 0) {
                  val_vals = decode_value(vv, val_types);
               } else {
                  for (auto t : val_types) {
                     val_vals.push_back(ColumnValue::null_value(t));
                  }
               }
            });
         } else {
            for (auto t : val_types) {
               val_vals.push_back(ColumnValue::null_value(t));
            }
         }
      }

      int64_t row_id = gstate.next_row_id++;
      txn.RegisterRowKey(gstate.root_index, row_id, std::string(key_view));

      for (duckdb::idx_t out_col = 0; out_col < lstate.column_ids.size(); out_col++) {
         auto col_idx = lstate.column_ids[out_col];
         if (col_idx == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
            duckdb::FlatVector::GetData<int64_t>(output.data[out_col])[count] = row_id;
            continue;
         }
         auto& loc = col_map[col_idx];
         const ColumnValue& cv = loc.in_pk ? pk_vals[loc.pos] : val_vals[loc.pos];
         write_column_to_vector(output.data[out_col], count, cv);
      }

      count++;
      mc.next();
   }

   if (mc.is_end()) {
      gstate.finished = true;
   }
   output.SetCardinality(count);
}

// ===========================================================================
// Complex filter pushdown: extract PK equality/range filters from expressions
// ===========================================================================

// Helper: extract comparison operands (column ref + constant) from a comparison expression
struct ComparisonOperands {
   duckdb::BoundColumnRefExpression* col_ref = nullptr;
   duckdb::BoundConstantExpression* const_ref = nullptr;
   duckdb::ExpressionType cmp;  // normalized: column on left side
};

static std::optional<ComparisonOperands>
extract_comparison(duckdb::Expression& expr) {
   if (expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL &&
       expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
       expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
       expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_LESSTHAN &&
       expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO) {
      return std::nullopt;
   }
   auto& comp = expr.Cast<duckdb::BoundComparisonExpression>();

   duckdb::BoundColumnRefExpression* col_ref = nullptr;
   duckdb::BoundConstantExpression* const_ref = nullptr;
   bool reversed = false;

   if (comp.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF &&
       comp.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
      col_ref = &comp.left->Cast<duckdb::BoundColumnRefExpression>();
      const_ref = &comp.right->Cast<duckdb::BoundConstantExpression>();
   } else if (comp.right->GetExpressionClass() == duckdb::ExpressionClass::BOUND_COLUMN_REF &&
              comp.left->GetExpressionClass() == duckdb::ExpressionClass::BOUND_CONSTANT) {
      col_ref = &comp.right->Cast<duckdb::BoundColumnRefExpression>();
      const_ref = &comp.left->Cast<duckdb::BoundConstantExpression>();
      reversed = true;
   } else {
      return std::nullopt;
   }

   auto cmp = expr.GetExpressionType();
   if (reversed) {
      switch (cmp) {
         case duckdb::ExpressionType::COMPARE_GREATERTHAN:
            cmp = duckdb::ExpressionType::COMPARE_LESSTHAN; break;
         case duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            cmp = duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO; break;
         case duckdb::ExpressionType::COMPARE_LESSTHAN:
            cmp = duckdb::ExpressionType::COMPARE_GREATERTHAN; break;
         case duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO:
            cmp = duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO; break;
         default: break;
      }
   }

   return ComparisonOperands{col_ref, const_ref, cmp};
}

void PsitriPushdownComplexFilter(duckdb::ClientContext& context,
                                 duckdb::LogicalGet& get,
                                 duckdb::FunctionData* bind_data_p,
                                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
   auto& bind_data = bind_data_p->Cast<PsitriScanBindData>();
   auto& meta = bind_data.meta;
   auto pk_indices = meta.pk_column_indices();
   if (pk_indices.empty()) return;

   // Build mapping: table_column_index -> binding index in LogicalGet
   std::unordered_map<uint32_t, duckdb::idx_t> col_to_binding;
   for (duckdb::idx_t i = 0; i < get.GetColumnIds().size(); i++) {
      col_to_binding[get.GetColumnIds()[i].GetPrimaryIndex()] = i;
   }

   // For each PK column, find an equality filter value (if any)
   // pk_eq_vals[i] = constant value for pk_indices[i], or nullopt
   std::vector<std::optional<duckdb::Value>> pk_eq_vals(pk_indices.size());

   // Also collect range filters on the first PK column (for single or composite PKs)
   duckdb::idx_t first_pk_binding = duckdb::DConstants::INVALID_INDEX;
   if (auto it = col_to_binding.find(pk_indices[0]); it != col_to_binding.end()) {
      first_pk_binding = it->second;
   }

   for (auto& filter_ptr : filters) {
      auto ops = extract_comparison(*filter_ptr);
      if (!ops) continue;

      auto binding_idx = ops->col_ref->binding.column_index;

      // Check each PK column for equality
      for (size_t pi = 0; pi < pk_indices.size(); pi++) {
         auto it = col_to_binding.find(pk_indices[pi]);
         if (it == col_to_binding.end()) continue;
         if (binding_idx == it->second && ops->cmp == duckdb::ExpressionType::COMPARE_EQUAL) {
            pk_eq_vals[pi] = ops->const_ref->value;
         }
      }

      // Range filters on first PK column
      if (first_pk_binding != duckdb::DConstants::INVALID_INDEX &&
          binding_idx == first_pk_binding) {
         auto pk_type = meta.columns[pk_indices[0]].type;
         auto cv = duckdb_value_to_column(ops->const_ref->value, pk_type);
         auto key = encode_key({cv});

         if (ops->cmp == duckdb::ExpressionType::COMPARE_GREATERTHAN ||
             ops->cmp == duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
            bind_data.has_lower_bound = true;
            bind_data.lower_bound_key = std::move(key);
         } else if (ops->cmp == duckdb::ExpressionType::COMPARE_LESSTHAN ||
                    ops->cmp == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO) {
            bind_data.has_upper_bound = true;
            bind_data.upper_bound_key = std::move(key);
         }
      }
   }

   // Count how many leading PK columns have equality filters
   size_t eq_prefix_len = 0;
   for (size_t i = 0; i < pk_indices.size(); i++) {
      if (pk_eq_vals[i].has_value()) {
         eq_prefix_len = i + 1;
      } else {
         break;
      }
   }

   if (eq_prefix_len == pk_indices.size()) {
      // All PK columns matched — point lookup
      std::vector<ColumnValue> key_vals;
      key_vals.reserve(pk_indices.size());
      for (size_t i = 0; i < pk_indices.size(); i++) {
         key_vals.push_back(duckdb_value_to_column(
            *pk_eq_vals[i], meta.columns[pk_indices[i]].type));
      }
      bind_data.has_pk_eq_filter = true;
      bind_data.pk_eq_key = encode_key(key_vals);
      // Clear range filters — point lookup is more specific
      bind_data.has_lower_bound = false;
      bind_data.has_upper_bound = false;
   } else if (eq_prefix_len > 0) {
      // Prefix of PK columns matched — prefix range scan
      std::vector<ColumnValue> prefix_vals;
      prefix_vals.reserve(eq_prefix_len);
      for (size_t i = 0; i < eq_prefix_len; i++) {
         prefix_vals.push_back(duckdb_value_to_column(
            *pk_eq_vals[i], meta.columns[pk_indices[i]].type));
      }
      bind_data.has_pk_prefix_filter = true;
      bind_data.pk_prefix_key = encode_key(prefix_vals);
      // Clear range filters — prefix scan is more specific
      bind_data.has_lower_bound = false;
      bind_data.has_upper_bound = false;
   }

   // If no PK filter was found, check secondary indexes for equality filters
   if (!bind_data.has_pk_eq_filter && !bind_data.has_pk_prefix_filter &&
       !meta.indexes.empty()) {
      for (auto& idx : meta.indexes) {
         // Try to match all index columns to equality filters
         std::vector<std::optional<duckdb::Value>> idx_eq_vals(idx.column_indices.size());
         bool all_matched = true;

         for (size_t ic = 0; ic < idx.column_indices.size(); ic++) {
            uint32_t table_col = idx.column_indices[ic];
            auto col_it = col_to_binding.find(table_col);
            if (col_it == col_to_binding.end()) { all_matched = false; break; }

            bool found = false;
            for (auto& filter_ptr : filters) {
               auto ops = extract_comparison(*filter_ptr);
               if (!ops) continue;
               if (ops->col_ref->binding.column_index == col_it->second &&
                   ops->cmp == duckdb::ExpressionType::COMPARE_EQUAL) {
                  idx_eq_vals[ic] = ops->const_ref->value;
                  found = true;
                  break;
               }
            }
            if (!found) { all_matched = false; break; }
         }

         if (all_matched) {
            // Encode the index key from the filter values
            std::vector<ColumnValue> idx_key_vals;
            idx_key_vals.reserve(idx.column_indices.size());
            for (size_t ic = 0; ic < idx.column_indices.size(); ic++) {
               uint32_t table_col = idx.column_indices[ic];
               idx_key_vals.push_back(duckdb_value_to_column(
                  *idx_eq_vals[ic], meta.columns[table_col].type));
            }
            bind_data.has_index_lookup = true;
            bind_data.index_root = idx.root_index;
            bind_data.index_lookup_key = encode_key(idx_key_vals);
            break;  // Use the first matching index
         }
      }
   }
}

} // namespace psitri_sql
