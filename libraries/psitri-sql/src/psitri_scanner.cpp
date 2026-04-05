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

#include <psitri/cursor.hpp>
#include <psitri/read_session_impl.hpp>
#include <psitri-sql/psitri_transaction.hpp>

namespace psitri_sql {

// ===========================================================================
// Bind data
// ===========================================================================

duckdb::unique_ptr<duckdb::FunctionData> PsitriScanBindData::Copy() const {
   auto result     = duckdb::make_uniq<PsitriScanBindData>();
   result->catalog = catalog;
   result->meta    = meta;
   result->has_pk_eq_filter = has_pk_eq_filter;
   result->pk_eq_key        = pk_eq_key;
   result->has_lower_bound  = has_lower_bound;
   result->has_upper_bound  = has_upper_bound;
   result->lower_bound_key  = lower_bound_key;
   result->upper_bound_key  = upper_bound_key;
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
   state->has_pk_eq_filter = bind_data.has_pk_eq_filter;
   state->pk_eq_key        = bind_data.pk_eq_key;
   state->has_lower_bound  = bind_data.has_lower_bound;
   state->has_upper_bound  = bind_data.has_upper_bound;
   state->lower_bound_key  = bind_data.lower_bound_key;
   state->upper_bound_key  = bind_data.upper_bound_key;

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

   // Initialize cursor on first call
   if (!lstate.initialized) {
      txn.ClearRowKeys(gstate.root_index);
      lstate.read_session = bind_data.catalog->GetStorage()->start_read_session();
      lstate.cursor.emplace(lstate.read_session->create_cursor(gstate.root_index));

      if (lstate.has_pk_eq_filter) {
         // Point lookup: seek to exact key
         lstate.cursor->lower_bound(lstate.pk_eq_key);
      } else if (lstate.has_lower_bound) {
         lstate.cursor->lower_bound(lstate.lower_bound_key);
      } else {
         lstate.cursor->seek_begin();
      }
      lstate.initialized = true;
   }
   auto& cursor = *lstate.cursor;

   duckdb::idx_t count     = 0;
   duckdb::idx_t max_count = STANDARD_VECTOR_SIZE;

   while (count < max_count && !cursor.is_end()) {
      auto key_view = cursor.key();

      // For PK equality filter: stop after the first non-matching key
      if (lstate.has_pk_eq_filter) {
         if (key_view != lstate.pk_eq_key) {
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

      // Zero-copy value read: decode directly from the value_view callback
      std::vector<ColumnValue> val_vals;
      cursor.get_value([&](psitri::value_view vv) {
         if (vv.size() > 0) {
            val_vals = decode_value(vv, val_types);
         } else {
            for (auto t : val_types) {
               val_vals.push_back(ColumnValue::null_value(t));
            }
         }
      });

      int64_t row_id = gstate.next_row_id++;
      // Register the encoded key so DELETE/UPDATE can find it by row_id
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
      cursor.next();
   }

   if (cursor.is_end()) {
      gstate.finished = true;
   }
   output.SetCardinality(count);
}

// ===========================================================================
// Complex filter pushdown: extract PK equality/range filters from expressions
// ===========================================================================

void PsitriPushdownComplexFilter(duckdb::ClientContext& context,
                                 duckdb::LogicalGet& get,
                                 duckdb::FunctionData* bind_data_p,
                                 duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
   auto& bind_data = bind_data_p->Cast<PsitriScanBindData>();
   auto& meta = bind_data.meta;
   auto pk_indices = meta.pk_column_indices();

   // Only handle single-column PK for now
   if (pk_indices.size() != 1) return;
   auto pk_col = pk_indices[0];
   auto pk_type = meta.columns[pk_col].type;

   // Find the column_id index for the PK column in the scan
   duckdb::idx_t pk_binding = duckdb::DConstants::INVALID_INDEX;
   for (duckdb::idx_t i = 0; i < get.GetColumnIds().size(); i++) {
      if (get.GetColumnIds()[i].GetPrimaryIndex() == pk_col) {
         pk_binding = i;
         break;
      }
   }
   if (pk_binding == duckdb::DConstants::INVALID_INDEX) return;

   for (auto it = filters.begin(); it != filters.end(); ) {
      auto& expr = **it;
      if (expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_EQUAL &&
          expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_GREATERTHAN &&
          expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO &&
          expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_LESSTHAN &&
          expr.GetExpressionType() != duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO) {
         ++it;
         continue;
      }
      auto& comp = expr.Cast<duckdb::BoundComparisonExpression>();

      // Check if one side is a column ref to PK and other is a constant
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
         ++it;
         continue;
      }

      // Check if this references the PK column
      if (col_ref->binding.column_index != pk_binding) {
         ++it;
         continue;
      }

      auto cv = duckdb_value_to_column(const_ref->value, pk_type);
      auto key = encode_key({cv});
      auto cmp = expr.GetExpressionType();
      if (reversed) {
         // Flip comparison direction when constant is on the left
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

      if (cmp == duckdb::ExpressionType::COMPARE_EQUAL) {
         bind_data.has_pk_eq_filter = true;
         bind_data.pk_eq_key = std::move(key);
         // Don't remove — DuckDB keeps as safety filter
      } else if (cmp == duckdb::ExpressionType::COMPARE_GREATERTHAN ||
                 cmp == duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
         bind_data.has_lower_bound = true;
         bind_data.lower_bound_key = std::move(key);
      } else if (cmp == duckdb::ExpressionType::COMPARE_LESSTHAN ||
                 cmp == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO) {
         bind_data.has_upper_bound = true;
         bind_data.upper_bound_key = std::move(key);
      }
      ++it;
      // We don't erase the filter — DuckDB keeps it for correctness
   }
}

} // namespace psitri_sql
