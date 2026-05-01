#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <hash/xxhash.h>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <psitri/live_range_map.hpp>
#include <psitri/lock_policy.hpp>
#include <psitri/node/inner.hpp>
#include <psitri/node/leaf.hpp>
#include <psitri/node/value_node.hpp>
#include <psitri/version_compare.hpp>
#include <psitri/write_session.hpp>
#include <sal/allocator.hpp>
#include <sal/config.hpp>
#include <sal/mapping.hpp>
#include <sal/seg_alloc_dump.hpp>
#include <sal/verify.hpp>
#include <thread>
#include <vector>

namespace psitri
{
   using runtime_config = sal::runtime_config;
   using recovery_mode  = sal::recovery_mode;

   template <class LockPolicy>
   class basic_write_session;

   template <class LockPolicy>
   class basic_read_session;

   struct version_audit_options;
   struct version_audit_result;
   struct version_index_audit_result;
   struct tree_stats_options;
   struct tree_stats_result;

   static constexpr uint32_t num_top_roots = 512;

   /// Controls how database::open() handles existing vs. new databases.
   enum class open_mode
   {
      /// Open an existing database or create a new one.
      create_or_open,

      /// Create a new database. Fails if the database already exists.
      create_only,

      /// Open an existing database. Fails if the database does not exist.
      open_existing,

      /// Open an existing database in read-only mode.
      /// Not yet implemented — reserved for future use.
      read_only,
   };

   namespace detail
   {
      class database_state;

      /// Register PsiTri node object operations with this SAL allocator instance.
      void register_node_types(sal::allocator& alloc);
      tree_stats_result collect_tree_stats(sal::allocator& alloc,
                                           const tree_stats_options& options);

      struct psitri_object_context
      {
         live_range_map& dead_versions;
      };

      template <typename T>
      class psitri_node_ops : public sal::static_object_type_ops<T>
      {
        public:
         explicit psitri_node_ops(psitri_object_context& ctx) : _ctx(ctx) {}

        protected:
         psitri_object_context& context() const noexcept { return _ctx; }

        private:
         psitri_object_context& _ctx;
      };

      class psitri_value_node_ops final : public psitri_node_ops<value_node>
      {
        public:
         using psitri_node_ops<value_node>::psitri_node_ops;

         void active_compact_to(const sal::alloc_header*          src,
                                sal::alloc_header*                compact_dst,
                                const sal::allocator_session_ptr& session) const noexcept override
         {
            if (try_prune_retained_floor_active(src, compact_dst, session))
               return;
            psitri_node_ops<value_node>::active_compact_to(src, compact_dst, session);
         }

         void passive_compact_to(const sal::alloc_header*   src,
                                 sal::alloc_header*         compact_dst,
                                 sal::pending_release_list& pending_releases) const noexcept override
         {
            if (try_prune_retained_floor_passive(src, compact_dst, pending_releases))
               return;
            psitri_node_ops<value_node>::passive_compact_to(src, compact_dst,
                                                            pending_releases);
         }

        private:
         bool try_prune_retained_floor_active(
             const sal::alloc_header*          src,
             sal::alloc_header*                compact_dst,
             const sal::allocator_session_ptr& session) const noexcept
         {
            const auto* value = static_cast<const value_node*>(src);
            const auto* snap  = context().dead_versions.load_snapshot();
            if (!snap || snap->oldest_retained_floor() == 0)
               return false;
            if (!can_prune_retained_floor(*value, snap->oldest_retained_floor()))
               return false;

            value_node::prune_floor_policy prune{snap->oldest_retained_floor()};
            std::vector<sal::ptr_address> pending_storage;
            try
            {
               pending_storage.reserve(1024);
            }
            catch (...)
            {
               return false;
            }
            sal::pending_release_list pending(pending_storage);
            if (!value->collect_pruned_references(prune, pending))
               return false;
            new (compact_dst)
                value_node(compact_dst->size(), compact_dst->address_seq(), value, prune);
            release_pending_refs(session, pending);
            return true;
         }

         bool try_prune_retained_floor_passive(
             const sal::alloc_header*   src,
             sal::alloc_header*         compact_dst,
             sal::pending_release_list& pending_releases) const noexcept
         {
            const auto* value = static_cast<const value_node*>(src);
            const auto* snap  = context().dead_versions.load_snapshot();
            if (!snap || snap->oldest_retained_floor() == 0)
               return false;
            if (!can_prune_retained_floor(*value, snap->oldest_retained_floor()))
               return false;

            value_node::prune_floor_policy prune{snap->oldest_retained_floor()};
            if (!value->collect_pruned_references(prune, pending_releases))
               return false;
            new (compact_dst)
                value_node(compact_dst->size(), compact_dst->address_seq(), value, prune);
            return true;
         }

         static void release_pending_refs(
             const sal::allocator_session_ptr& session,
             const sal::pending_release_list&  pending) noexcept
         {
            for (auto adr : pending)
               session->release(adr);
         }

         static bool can_prune_retained_floor(const value_node& value,
                                              uint64_t floor) noexcept
         {
            if (value.is_flat() || value.num_next() != 0 || value.num_versions() == 0)
               return false;

            const uint64_t base           = value.get_entry_version(0);
            const uint64_t floor_token    = version_token(floor, value_version_bits);
            const uint64_t floor_distance = version_distance(base, floor_token,
                                                             value_version_bits);
            const uint64_t half_range     = version_mask(value_version_bits) >> 1;
            if (floor_distance > half_range)
               return false;

            uint8_t floor_idx = 0xFF;
            for (uint8_t i = 0; i < value.num_versions(); ++i)
            {
               uint64_t entry_distance =
                   version_distance(base, value.get_entry_version(i), value_version_bits);
               if (entry_distance <= floor_distance)
                  floor_idx = i;
               else
                  break;
            }

            if (floor_idx == 0xFF)
               return false;
            return floor_idx != 0 || value.get_entry_version(floor_idx) != floor_token;
         }
      };

      struct psitri_object_registry
      {
         explicit psitri_object_registry(live_range_map& dead)
             : context{dead},
               leaf(context),
               inner(context),
               inner_prefix(context),
               wide_inner(context),
               direct_inner(context),
               bplus_inner(context),
               value(context)
         {
         }

         void install(sal::allocator& alloc) noexcept
         {
            alloc.register_type_ops<leaf_node>(leaf);
            alloc.register_type_ops<inner_node>(inner);
            alloc.register_type_ops<inner_prefix_node>(inner_prefix);
            alloc.register_type_ops<wide_inner_node>(wide_inner);
            alloc.register_type_ops<direct_inner_node>(direct_inner);
            alloc.register_type_ops<bplus_inner_node>(bplus_inner);
            alloc.register_type_ops<value_node>(value);
         }

         psitri_object_context               context;
         psitri_node_ops<leaf_node>          leaf;
         psitri_node_ops<inner_node>         inner;
         psitri_node_ops<inner_prefix_node>  inner_prefix;
         psitri_node_ops<wide_inner_node>    wide_inner;
         psitri_node_ops<direct_inner_node>  direct_inner;
         psitri_node_ops<bplus_inner_node>   bplus_inner;
         psitri_value_node_ops               value;
      };

      /// Walk the tree from all top-level roots and populate a verify_result.
      /// Non-template so it can live in a .cpp file and keep the tree-walk
      /// logic out of the header.
      sal::verify_result verify_all_roots(sal::allocator& alloc);
      version_audit_result audit_all_roots(sal::allocator& alloc,
                                           const version_audit_options& options);
   }  // namespace detail

   /**
    * @brief High-level database statistics for monitoring and diagnostics.
    *
    * Returned by database::get_stats(). All fields are a point-in-time snapshot;
    * values may change between the call and when you read them.
    */
   struct database_stats
   {
      /** @name Storage */
      ///@{
      uint64_t total_segments         = 0;  ///< Number of 32 MB segments in the data file.
      uint64_t total_live_bytes       = 0;  ///< Bytes occupied by live (reachable) objects.
      uint64_t total_live_objects     = 0;  ///< Number of live objects across all segments.
      uint64_t total_free_bytes       = 0;  ///< Bytes reclaimable by compaction.
      uint64_t database_file_bytes    = 0;  ///< Total data file size on disk (segments x 32 MB).
      ///@}

      /** @name Cache */
      ///@{
      uint32_t pinned_segments        = 0;  ///< Segments currently mlock'd in RAM.
      uint64_t pinned_bytes           = 0;  ///< Total bytes in pinned segments (pinned_segments x 32 MB).
      uint64_t successful_mlock_regions = 0;  ///< Cumulative successful mlock() regions.
      uint64_t failed_mlock_regions     = 0;  ///< Cumulative failed mlock() regions.
      uint64_t successful_munlock_regions = 0; ///< Cumulative successful munlock() regions.
      uint64_t failed_munlock_regions     = 0; ///< Cumulative failed munlock() regions.
      uint64_t cache_difficulty       = 0;  ///< Current MFU promotion difficulty (self-tuning).
      uint64_t cache_policy_satisfied_bytes = 0; ///< Bytes counted by MFU policy controller.
      uint64_t total_promoted_bytes   = 0;  ///< Cumulative bytes promoted to pinned cache.
      uint64_t target_promoted_bytes_per_s = 0;  ///< MFU controller target promotion rate.
      uint64_t cache_hot_to_hot_promotions      = 0;  ///< Successful HOT -> HOT refreshes.
      uint64_t cache_hot_to_hot_promoted_bytes  = 0;  ///< Bytes copied by HOT -> HOT refreshes.
      uint64_t cache_hot_to_hot_demote_pressure_ppm = 0; ///< HOT refresh source age pressure.
      uint64_t cache_hot_to_hot_byte_demote_pressure_ppm = 0; ///< Byte-weighted source pressure.
      uint64_t cache_young_hot_skips      = 0;  ///< HOT advisories skipped while still young.
      uint64_t cache_young_hot_skipped_bytes = 0; ///< Bytes skipped by young-HOT filter.
      uint64_t cache_young_hot_skip_demote_pressure_ppm = 0; ///< Skip source age pressure.
      uint64_t cache_young_hot_skip_byte_demote_pressure_ppm = 0; ///< Byte-weighted skip pressure.
      uint64_t cache_cold_to_hot_promotions     = 0;  ///< Successful COLD -> HOT promotions.
      uint64_t cache_cold_to_hot_promoted_bytes = 0;  ///< Bytes copied by COLD -> HOT promotions.
      uint64_t cache_promoted_to_cold_promotions = 0; ///< Promotion attempts that landed cold.
      uint64_t cache_promoted_to_cold_bytes      = 0; ///< Bytes copied by cold-destination moves.
      ///@}

      /** @name Control Blocks */
      ///@{
      uint32_t control_block_zones    = 0;  ///< Number of mapped 32 MB control-block zones.
      uint64_t control_block_capacity = 0;  ///< Maximum addressable control blocks.
      bool     control_block_header_pinned = false;
      uint64_t control_block_zone_mlock_success_regions = 0;
      uint64_t control_block_zone_mlock_failed_regions  = 0;
      uint64_t control_block_zone_mlock_skipped_regions = 0;
      uint64_t control_block_zone_mlock_success_bytes   = 0;
      uint64_t control_block_freelist_mlock_success_regions = 0;
      uint64_t control_block_freelist_mlock_failed_regions  = 0;
      uint64_t control_block_freelist_mlock_skipped_regions = 0;
      uint64_t control_block_freelist_mlock_success_bytes   = 0;
      ///@}

      /** @name Sessions */
      ///@{
      uint32_t active_sessions        = 0;  ///< Number of active allocator sessions (read + write).
      int64_t  pending_releases       = 0;  ///< Objects queued for deferred deallocation by the compactor.
      ///@}

      /** @name Recycling */
      ///@{
      uint64_t recycled_queue_depth   = 0;  ///< Segments waiting for read locks to release before reuse.
      uint64_t recycled_queue_capacity= 0;  ///< Maximum capacity of the recycled-segments queue.
      ///@}

      /**
       * @brief Format the stats as a human-readable multi-line string.
       */
      std::string to_string() const
      {
         auto fmt_bytes = [](uint64_t b) -> std::string {
            char buf[64];
            if (b >= 1024ull * 1024 * 1024)
               snprintf(buf, sizeof(buf), "%.2f GB", b / (1024.0 * 1024.0 * 1024.0));
            else if (b >= 1024ull * 1024)
               snprintf(buf, sizeof(buf), "%.2f MB", b / (1024.0 * 1024.0));
            else if (b >= 1024ull)
               snprintf(buf, sizeof(buf), "%.2f KB", b / 1024.0);
            else
               snprintf(buf, sizeof(buf), "%llu B", (unsigned long long)b);
            return buf;
         };

         std::string s;
         s += "Storage:\n";
         s += "  segments:        " + std::to_string(total_segments) + "\n";
         s += "  live objects:    " + std::to_string(total_live_objects) + "\n";
         s += "  live data:       " + fmt_bytes(total_live_bytes) + "\n";
         s += "  free space:      " + fmt_bytes(total_free_bytes) + "\n";
         s += "  file size:       " + fmt_bytes(database_file_bytes) + "\n";
         s += "Cache:\n";
         s += "  pinned segments: " + std::to_string(pinned_segments) + "\n";
         s += "  pinned memory:   " + fmt_bytes(pinned_bytes) + "\n";
         s += "  mlock success:   " + std::to_string(successful_mlock_regions) +
              " / " + fmt_bytes(successful_mlock_regions * sal::segment_size) + "\n";
         if (failed_mlock_regions != 0)
            s += "  mlock failed:    " + std::to_string(failed_mlock_regions) + "\n";
         s += "  difficulty:      " + std::to_string(cache_difficulty) + "\n";
         s += "  policy bytes:    " + fmt_bytes(cache_policy_satisfied_bytes) + "\n";
         s += "  promoted:        " + fmt_bytes(total_promoted_bytes) + "\n";
         s += "  cold->hot:       " + std::to_string(cache_cold_to_hot_promotions) +
              " / " + fmt_bytes(cache_cold_to_hot_promoted_bytes) + "\n";
         s += "  hot refresh:     " + std::to_string(cache_hot_to_hot_promotions) +
              " / " + fmt_bytes(cache_hot_to_hot_promoted_bytes) + "\n";
         s += "  young skip:      " + std::to_string(cache_young_hot_skips) +
              " / " + fmt_bytes(cache_young_hot_skipped_bytes) + "\n";
         if (cache_hot_to_hot_promotions != 0)
         {
            char pressure[128];
            double avg_pressure =
                double(cache_hot_to_hot_demote_pressure_ppm) /
                double(cache_hot_to_hot_promotions) / 10000.0;
            double byte_avg_pressure =
                cache_hot_to_hot_promoted_bytes == 0
                    ? 0.0
                    : double(cache_hot_to_hot_byte_demote_pressure_ppm) /
                          double(cache_hot_to_hot_promoted_bytes) / 10000.0;
            snprintf(pressure, sizeof(pressure), "  refresh pressure: %.1f%% avg, %.1f%% byte-avg\n",
                     avg_pressure, byte_avg_pressure);
            s += pressure;
         }
         if (cache_promoted_to_cold_promotions != 0)
            s += "  promoted cold:   " + std::to_string(cache_promoted_to_cold_promotions) +
                 " / " + fmt_bytes(cache_promoted_to_cold_bytes) + "\n";
         s += "  target rate:     " + fmt_bytes(target_promoted_bytes_per_s) + "/s\n";
         s += "Control blocks:\n";
         s += "  zones:           " + std::to_string(control_block_zones) + "\n";
         s += "  capacity:        " + std::to_string(control_block_capacity) + "\n";
         s += "  header mlock:    " +
              std::string(control_block_header_pinned ? "yes" : "no") + "\n";
         s += "  zone mlock:      " +
              std::to_string(control_block_zone_mlock_success_regions) + " ok, " +
              std::to_string(control_block_zone_mlock_failed_regions) + " failed, " +
              std::to_string(control_block_zone_mlock_skipped_regions) + " skipped / " +
              fmt_bytes(control_block_zone_mlock_success_bytes) + "\n";
         s += "  free mlock:      " +
              std::to_string(control_block_freelist_mlock_success_regions) + " ok, " +
              std::to_string(control_block_freelist_mlock_failed_regions) + " failed, " +
              std::to_string(control_block_freelist_mlock_skipped_regions) + " skipped / " +
              fmt_bytes(control_block_freelist_mlock_success_bytes) + "\n";
         s += "Sessions:\n";
         s += "  active:          " + std::to_string(active_sessions) + "\n";
         s += "  pending releases:" + std::to_string(pending_releases) + "\n";
         return s;
      }

      friend std::ostream& operator<<(std::ostream& os, const database_stats& s)
      {
         return os << s.to_string();
      }
   };

   struct version_audit_options
   {
      /// Explicit oldest version that must remain readable. When zero, the
      /// audit uses the minimum valid version among populated top roots.
      uint64_t prune_floor = 0;

      /// Optional progress callback. The audit calls this after each
      /// progress_interval_nodes newly visited nodes. A zero interval disables
      /// progress reporting.
      uint64_t progress_interval_nodes = 0;
      void (*progress)(const version_audit_result&, void*) = nullptr;
      void* progress_user = nullptr;
   };

   struct version_audit_result
   {
      uint64_t requested_prune_floor = 0;
      uint64_t effective_prune_floor = 0;

      uint32_t roots_checked          = 0;
      uint32_t roots_with_version     = 0;
      uint32_t roots_without_version  = 0;
      uint64_t oldest_root_version    = 0;
      uint64_t newest_root_version    = 0;

      uint64_t nodes_visited          = 0;
      uint64_t shared_nodes_skipped   = 0;
      uint64_t dangling_pointers      = 0;
      uint64_t inner_nodes            = 0;
      uint64_t inner_prefix_nodes     = 0;
      uint64_t leaf_nodes             = 0;
      uint64_t value_nodes            = 0;
      uint64_t flat_value_nodes       = 0;

      uint64_t leaf_branches              = 0;
      uint64_t leaf_version_table_entries = 0;
      uint64_t leaf_branch_versions       = 0;
      uint64_t leaf_subtrees              = 0;
      uint64_t leaf_subtrees_without_ver  = 0;

      uint64_t value_entries              = 0;
      uint64_t value_nodes_with_history   = 0;
      uint64_t value_nodes_with_next      = 0;
      uint64_t value_next_ptrs            = 0;
      uint64_t subtree_value_nodes        = 0;
      uint64_t subtree_value_entries      = 0;
      uint64_t max_value_entries          = 0;

      uint64_t retained_versions          = 0;
      uint64_t value_versions_seen        = 0;
      uint64_t leaf_versions_seen         = 0;

      uint64_t prunable_value_entries     = 0;
      uint64_t prunable_value_nodes       = 0;
      uint64_t floor_rewrite_entries      = 0;
      uint64_t prune_floor_unknown_nodes  = 0;
      uint64_t prune_floor_out_of_range_nodes = 0;
   };

   struct version_index_audit_result
   {
      uint64_t latest_version                 = 0;
      uint64_t retained_versions_from_index  = 0;
      uint64_t dead_versions_from_index      = 0;
      uint64_t dead_version_ranges           = 0;
      uint64_t pending_dead_versions         = 0;

	      uint64_t version_control_blocks        = 0;
	      uint64_t live_version_control_blocks   = 0;
	      uint64_t unknown_live_version_blocks   = 0;
	      uint64_t zero_ref_version_blocks       = 0;
	      uint64_t min_live_version              = 0;
	      uint64_t max_live_version              = 0;

	      bool index_matches_control_blocks() const noexcept
	      {
	         return unknown_live_version_blocks == 0 &&
	                retained_versions_from_index == live_version_control_blocks;
	      }
   };

   struct tree_stats_fanout_buckets
   {
      uint64_t fanout_1        = 0;
      uint64_t fanout_2        = 0;
      uint64_t fanout_3_to_4   = 0;
      uint64_t fanout_5_to_8   = 0;
      uint64_t fanout_9_to_16  = 0;
      uint64_t fanout_17_to_32 = 0;
      uint64_t fanout_33_to_64 = 0;
      uint64_t fanout_65_to_128 = 0;
      uint64_t fanout_129_plus = 0;

      void record(uint64_t branches) noexcept
      {
         if (branches <= 1)
            ++fanout_1;
         else if (branches == 2)
            ++fanout_2;
         else if (branches <= 4)
            ++fanout_3_to_4;
         else if (branches <= 8)
            ++fanout_5_to_8;
         else if (branches <= 16)
            ++fanout_9_to_16;
         else if (branches <= 32)
            ++fanout_17_to_32;
         else if (branches <= 64)
            ++fanout_33_to_64;
         else if (branches <= 128)
            ++fanout_65_to_128;
         else
            ++fanout_129_plus;
      }

      uint64_t total() const noexcept
      {
         return fanout_1 + fanout_2 + fanout_3_to_4 + fanout_5_to_8 +
                fanout_9_to_16 + fanout_17_to_32 + fanout_33_to_64 +
                fanout_65_to_128 + fanout_129_plus;
      }
   };

   struct tree_stats_depth_row
   {
      uint32_t depth = 0;

      uint64_t inner_nodes        = 0;
      uint64_t inner_prefix_nodes = 0;
      uint64_t value_nodes        = 0;
      uint64_t flat_value_nodes   = 0;

      uint64_t inner_branches       = 0;
      uint64_t single_branch_inners = 0;
      uint64_t low_fanout_inners    = 0;
      tree_stats_fanout_buckets fanout;

      uint64_t leaf_nodes             = 0;
      uint64_t leaf_keys              = 0;
      uint64_t selected_leaf_keys     = 0;
      uint64_t key_bytes              = 0;
      uint64_t selected_key_bytes     = 0;
      uint64_t max_key_size           = 0;
      uint64_t max_selected_key_size  = 0;
      uint64_t data_value_count       = 0;
      uint64_t data_value_bytes       = 0;
      uint64_t max_data_value_size    = 0;
      uint64_t leaf_clines            = 0;
      uint64_t max_leaf_clines        = 0;
      uint64_t cline_saturated_leaves = 0;
      uint64_t leaf_address_values    = 0;
      uint64_t leaf_alloc_bytes       = 0;
      uint64_t leaf_used_bytes        = 0;
      uint64_t leaf_dead_bytes        = 0;
      uint64_t leaf_empty_bytes       = 0;
      uint64_t full_leaf_nodes        = 0;
      uint64_t full_leaf_dead_bytes   = 0;
      uint64_t full_leaf_empty_bytes  = 0;

      uint64_t total_inner_nodes() const noexcept
      {
         return inner_nodes + inner_prefix_nodes;
      }

      double average_branches_per_inner() const noexcept
      {
         auto inners = total_inner_nodes();
         return inners ? double(inner_branches) / double(inners) : 0.0;
      }

      double average_keys_per_leaf() const noexcept
      {
         return leaf_nodes ? double(leaf_keys) / double(leaf_nodes) : 0.0;
      }

      double average_key_size() const noexcept
      {
         return leaf_keys ? double(key_bytes) / double(leaf_keys) : 0.0;
      }

      double average_selected_key_size() const noexcept
      {
         return selected_leaf_keys ? double(selected_key_bytes) / double(selected_leaf_keys)
                                   : 0.0;
      }

      double average_data_value_size() const noexcept
      {
         return data_value_count ? double(data_value_bytes) / double(data_value_count) : 0.0;
      }

      double average_clines_per_leaf() const noexcept
      {
         return leaf_nodes ? double(leaf_clines) / double(leaf_nodes) : 0.0;
      }

      double average_address_values_per_leaf() const noexcept
      {
         return leaf_nodes ? double(leaf_address_values) / double(leaf_nodes) : 0.0;
      }

      double average_address_values_per_cline() const noexcept
      {
         return leaf_clines ? double(leaf_address_values) / double(leaf_clines) : 0.0;
      }

      double leaf_dead_space_percent() const noexcept
      {
         return leaf_alloc_bytes ? 100.0 * double(leaf_dead_bytes) / double(leaf_alloc_bytes)
                                 : 0.0;
      }

      double leaf_empty_space_percent() const noexcept
      {
         return leaf_alloc_bytes ? 100.0 * double(leaf_empty_bytes) / double(leaf_alloc_bytes)
                                 : 0.0;
      }
   };

   struct tree_stats_options
   {
      /// Optional top-root index to scan. When unset, all populated top roots
      /// are scanned.
      std::optional<uint32_t> root_index;

      /// Optional inclusive lower key bound. Empty string is a valid key bound,
      /// so absence is represented by std::nullopt.
      std::optional<std::string> key_lower;

      /// Optional exclusive upper key bound.
      std::optional<std::string> key_upper;

      /// Optional progress callback. Called every progress_interval_nodes
      /// newly visited nodes. A zero interval disables progress reporting.
      uint64_t progress_interval_nodes = 0;
      void (*progress)(const tree_stats_result&, void*) = nullptr;
      void* progress_user = nullptr;

      /// Optional hard cap on visited nodes. A zero cap means scan until the
      /// requested roots/ranges are exhausted.
      uint64_t max_nodes = 0;

      bool has_key_range() const noexcept { return key_lower || key_upper; }
   };

   struct tree_stats_result
   {
      bool     key_range_enabled       = false;
      bool     root_filter_enabled     = false;
      uint32_t root_filter_index       = 0;
      std::string key_range_lower;
      std::string key_range_upper;
      bool     scan_truncated          = false;
      uint64_t max_nodes               = 0;

      uint32_t roots_checked          = 0;
      uint32_t roots_with_version     = 0;
      uint32_t roots_without_version  = 0;

      uint64_t latest_version           = 0;
      uint64_t retained_versions        = 0;
      uint64_t dead_versions            = 0;
      uint64_t dead_version_ranges      = 0;
      uint64_t pending_dead_versions    = 0;

      uint64_t nodes_visited          = 0;
      uint64_t shared_nodes_skipped   = 0;
      uint64_t dangling_pointers      = 0;
      uint64_t reachable_bytes        = 0;

      uint64_t inner_nodes            = 0;
      uint64_t inner_prefix_nodes     = 0;
      uint64_t leaf_nodes             = 0;
      uint64_t value_nodes            = 0;
      uint64_t flat_value_nodes       = 0;

      uint64_t inner_branches         = 0;
      uint64_t single_branch_inners   = 0;
      uint64_t low_fanout_inners      = 0;
      uint64_t leaf_keys              = 0;
      uint64_t selected_leaf_keys      = 0;
      uint64_t key_bytes              = 0;
      uint64_t selected_key_bytes     = 0;
      uint64_t max_key_size           = 0;
      uint64_t max_selected_key_size  = 0;
      uint64_t data_value_count       = 0;
      uint64_t data_value_bytes       = 0;
      uint64_t max_data_value_size    = 0;
      uint64_t leaf_clines            = 0;
      uint64_t max_leaf_clines        = 0;
      uint64_t cline_saturated_leaves = 0;
      uint64_t leaf_address_values    = 0;
      uint64_t max_depth              = 0;
      uint64_t leaf_depth_sum         = 0;
      uint64_t key_depth_sum          = 0;

      uint64_t total_inner_bytes      = 0;
      uint64_t total_leaf_alloc_bytes = 0;
      uint64_t total_leaf_used_bytes  = 0;
      uint64_t total_leaf_dead_bytes  = 0;
      uint64_t total_leaf_empty_bytes = 0;
      uint64_t full_leaf_nodes        = 0;
      uint64_t full_leaf_dead_bytes   = 0;
      uint64_t full_leaf_empty_bytes  = 0;
      uint64_t total_value_bytes      = 0;

      std::vector<uint64_t> branches_per_inner_node;
      std::vector<uint64_t> keys_per_leaf;
      std::vector<uint64_t> leaf_clines_histogram;
      std::vector<uint64_t> address_values_per_leaf;
      std::vector<uint64_t> leaf_depths;
      std::vector<tree_stats_depth_row> depth_stats;

      uint64_t total_inner_nodes() const noexcept
      {
         return inner_nodes + inner_prefix_nodes;
      }

      double average_depth() const noexcept
      {
         return leaf_keys ? double(key_depth_sum) / double(leaf_keys) : 0.0;
      }

      double average_leaf_depth() const noexcept
      {
         return leaf_nodes ? double(leaf_depth_sum) / double(leaf_nodes) : 0.0;
      }

      double average_leaf_alloc_size() const noexcept
      {
         return leaf_nodes ? double(total_leaf_alloc_bytes) / double(leaf_nodes) : 0.0;
      }

      double average_leaf_used_size() const noexcept
      {
         return leaf_nodes ? double(total_leaf_used_bytes) / double(leaf_nodes) : 0.0;
      }

      double average_leaf_dead_space() const noexcept
      {
         return leaf_nodes ? double(total_leaf_dead_bytes) / double(leaf_nodes) : 0.0;
      }

      double average_leaf_empty_space() const noexcept
      {
         return leaf_nodes ? double(total_leaf_empty_bytes) / double(leaf_nodes) : 0.0;
      }

      double leaf_dead_space_percent() const noexcept
      {
         return total_leaf_alloc_bytes
                    ? 100.0 * double(total_leaf_dead_bytes) / double(total_leaf_alloc_bytes)
                    : 0.0;
      }

      double leaf_empty_space_percent() const noexcept
      {
         return total_leaf_alloc_bytes
                    ? 100.0 * double(total_leaf_empty_bytes) / double(total_leaf_alloc_bytes)
                    : 0.0;
      }

      double average_full_leaf_dead_space() const noexcept
      {
         return full_leaf_nodes ? double(full_leaf_dead_bytes) / double(full_leaf_nodes) : 0.0;
      }

      double average_full_leaf_empty_space() const noexcept
      {
         return full_leaf_nodes ? double(full_leaf_empty_bytes) / double(full_leaf_nodes) : 0.0;
      }

      double full_leaf_dead_space_percent() const noexcept
      {
         const uint64_t full_leaf_bytes = full_leaf_nodes * uint64_t(leaf_node::max_leaf_size);
         return full_leaf_bytes ? 100.0 * double(full_leaf_dead_bytes) / double(full_leaf_bytes)
                                : 0.0;
      }

      double full_leaf_empty_space_percent() const noexcept
      {
         const uint64_t full_leaf_bytes = full_leaf_nodes * uint64_t(leaf_node::max_leaf_size);
         return full_leaf_bytes ? 100.0 * double(full_leaf_empty_bytes) / double(full_leaf_bytes)
                                : 0.0;
      }

      double average_keys_per_leaf() const noexcept
      {
         return leaf_nodes ? double(leaf_keys) / double(leaf_nodes) : 0.0;
      }

      double average_key_size() const noexcept
      {
         return leaf_keys ? double(key_bytes) / double(leaf_keys) : 0.0;
      }

      double average_selected_key_size() const noexcept
      {
         return selected_leaf_keys ? double(selected_key_bytes) / double(selected_leaf_keys)
                                   : 0.0;
      }

      double average_data_value_size() const noexcept
      {
         return data_value_count ? double(data_value_bytes) / double(data_value_count) : 0.0;
      }

      double average_clines_per_leaf() const noexcept
      {
         return leaf_nodes ? double(leaf_clines) / double(leaf_nodes) : 0.0;
      }

      double average_address_values_per_leaf() const noexcept
      {
         return leaf_nodes ? double(leaf_address_values) / double(leaf_nodes) : 0.0;
      }

      double average_address_values_per_cline() const noexcept
      {
         return leaf_clines ? double(leaf_address_values) / double(leaf_clines) : 0.0;
      }

      double average_branches_per_inner() const noexcept
      {
         auto inners = total_inner_nodes();
         return inners ? double(inner_branches) / double(inners) : 0.0;
      }

      tree_stats_depth_row& row_for_depth(uint32_t depth)
      {
         if (depth_stats.size() <= depth)
         {
            auto old_size = depth_stats.size();
            depth_stats.resize(size_t(depth) + 1);
            for (size_t i = old_size; i < depth_stats.size(); ++i)
               depth_stats[i].depth = uint32_t(i);
         }
         return depth_stats[depth];
      }
   };

   /**
    * @brief The main entry point for creating and managing a PsiTri database.
    *
    * A database manages the on-disk storage, background threads (compactor,
    * segment provider, read-bit decay), and up to 512 independent top-level
    * roots. All reads and writes go through sessions obtained from this class.
    *
    * @tparam LockPolicy  Policy type whose `mutex_type` alias selects the
    *                    mutex used for internal synchronization. Defaults to
    *                    `std_lock_policy` (std::mutex). Fiber-based callers
    *                    should supply a policy whose mutex yields instead of
    *                    blocking the OS thread on contention.
    *
    * Typical usage (default policy):
    * @code
    *   auto db = psitri::database::open("mydb", psitri::open_mode::create_or_open);
    *   auto ws = db->start_write_session();
    *   auto tx = ws->start_transaction(0);
    *   tx.upsert("key", "value");
    *   tx.commit();
    * @endcode
    */
   template <class LockPolicy = std_lock_policy>
   class basic_database : public std::enable_shared_from_this<basic_database<LockPolicy>>
   {
     public:
      using lock_policy_type = LockPolicy;
      using mutex_type       = typename LockPolicy::mutex_type;
      using write_session_type = basic_write_session<LockPolicy>;
      using read_session_type  = basic_read_session<LockPolicy>;

      /** @name Construction & Lifecycle */
      ///@{

      /**
       * @brief Open or create a database.
       *
       * @param dir   Directory containing (or to contain) the database files.
       * @param mode  How to handle existing vs. new databases.
       * @param cfg   Runtime configuration (cache budget, sync mode, etc.).
       * @return A shared_ptr to the database.
       */
      static std::shared_ptr<basic_database> open(
          std::filesystem::path dir,
          open_mode             mode     = open_mode::create_or_open,
          const runtime_config& cfg      = {},
          recovery_mode         recovery = recovery_mode::none);

      /**
       * @brief Create-only convenience helper. Fails if the database already exists.
       *
       * Equivalent to database::open(dir, open_mode::create_only, cfg).
       * Prefer database::open() in user-facing examples so the open mode is
       * visible at the call site.
       */
      static std::shared_ptr<basic_database> create(std::filesystem::path dir,
                                                    const runtime_config& = {});

      ~basic_database();

      ///@}

      /// @cond INTERNAL
      /**
       * @brief Low-level constructor. Prefer database::open() for normal use.
       */
      basic_database(const std::filesystem::path& dir,
                     const runtime_config&        cfg,
                     recovery_mode                mode = recovery_mode::none);
      /// @endcond

      /** @name Sessions */
      ///@{

      std::shared_ptr<write_session_type> start_write_session();
      std::shared_ptr<read_session_type>  start_read_session();

      ///@}

      /** @name Configuration */
      ///@{

	      void sync()
	      {
	         std::lock_guard<mutex_type> lock(_sync_mutex);
	         _dead_versions.flush_pending();
	         _dead_versions.publish_snapshot();
	         _dead_versions.sync(_cfg.sync_mode);
	         _allocator.sync(_cfg.sync_mode);
	      }

      void set_runtime_config(const runtime_config& cfg)
      {
         _cfg = cfg;
         _allocator.set_runtime_config(cfg);
      }

      ///@}

      /** @name Statistics */
      ///@{

      database_stats get_stats() const
      {
         auto d = _allocator.dump();
         database_stats s;
         s.total_segments       = d.total_segments;
         s.total_live_bytes     = d.total_read_bytes;
         // total_read_nodes from the segment dump only counts objects in
         // their last-known segment location; objects relocated by the
         // compactor are double-counted/missed there. Pull the live count
         // from the control-block allocator instead.
         s.total_live_objects   = _allocator.total_allocated_objects();
         s.total_free_bytes     = d.total_free_space;
         s.database_file_bytes  = d.total_segments * sal::segment_size;
         s.pinned_segments      = d.mlocked_segments_count;
         s.pinned_bytes         = uint64_t(d.mlocked_segments_count) * sal::segment_size;
         s.successful_mlock_regions = d.successful_mlock_regions;
         s.failed_mlock_regions = d.failed_mlock_regions;
         s.successful_munlock_regions = d.successful_munlock_regions;
         s.failed_munlock_regions = d.failed_munlock_regions;
         s.cache_difficulty     = d.cache_difficulty;
         s.cache_policy_satisfied_bytes = d.cache_policy_satisfied_bytes;
         s.total_promoted_bytes = d.total_promoted_bytes;
         s.target_promoted_bytes_per_s = d.cache_target_promoted_bytes_per_s;
         s.cache_hot_to_hot_promotions = d.cache_hot_to_hot_promotions;
         s.cache_hot_to_hot_promoted_bytes = d.cache_hot_to_hot_promoted_bytes;
         s.cache_hot_to_hot_demote_pressure_ppm = d.cache_hot_to_hot_demote_pressure_ppm;
         s.cache_hot_to_hot_byte_demote_pressure_ppm =
             d.cache_hot_to_hot_byte_demote_pressure_ppm;
         s.cache_young_hot_skips = d.cache_young_hot_skips;
         s.cache_young_hot_skipped_bytes = d.cache_young_hot_skipped_bytes;
         s.cache_young_hot_skip_demote_pressure_ppm =
             d.cache_young_hot_skip_demote_pressure_ppm;
         s.cache_young_hot_skip_byte_demote_pressure_ppm =
             d.cache_young_hot_skip_byte_demote_pressure_ppm;
         s.cache_cold_to_hot_promotions = d.cache_cold_to_hot_promotions;
         s.cache_cold_to_hot_promoted_bytes = d.cache_cold_to_hot_promoted_bytes;
         s.cache_promoted_to_cold_promotions = d.cache_promoted_to_cold_promotions;
         s.cache_promoted_to_cold_bytes = d.cache_promoted_to_cold_bytes;
         s.control_block_zones    = d.control_block_zones;
         s.control_block_capacity = d.control_block_capacity;
         s.control_block_header_pinned = d.control_block_header_pinned;
         s.control_block_zone_mlock_success_regions =
             d.control_block_zone_mlock_success_regions;
         s.control_block_zone_mlock_failed_regions = d.control_block_zone_mlock_failed_regions;
         s.control_block_zone_mlock_skipped_regions =
             d.control_block_zone_mlock_skipped_regions;
         s.control_block_zone_mlock_success_bytes = d.control_block_zone_mlock_success_bytes;
         s.control_block_freelist_mlock_success_regions =
             d.control_block_freelist_mlock_success_regions;
         s.control_block_freelist_mlock_failed_regions =
             d.control_block_freelist_mlock_failed_regions;
         s.control_block_freelist_mlock_skipped_regions =
             d.control_block_freelist_mlock_skipped_regions;
         s.control_block_freelist_mlock_success_bytes =
             d.control_block_freelist_mlock_success_bytes;
         s.active_sessions      = d.active_sessions;
         s.pending_releases     = d.free_release_count;
         s.recycled_queue_depth    = d.recycled_queue_depth;
         s.recycled_queue_capacity = d.recycled_queue_capacity;
         return s;
      }

      ///@}

      /** @name MVCC Configuration */
      ///@{

      /// Set the epoch interval (number of global versions per epoch).
      /// Smaller values cause more frequent COW maintenance passes;
      /// larger values allow more MVCC fast-path writes before cleanup.
      void set_epoch_interval(uint64_t interval);

      /// Current epoch base in logical version space.
      uint64_t current_epoch_base() const;

      ///@}

      /** @name Compaction & Maintenance */
      ///@{

      bool wait_for_compactor(std::chrono::milliseconds timeout = std::chrono::milliseconds(10000))
      {
         auto deadline = std::chrono::steady_clock::now() + timeout;
         while (std::chrono::steady_clock::now() < deadline)
         {
            if (_allocator.total_pending_releases() == 0)
            {
               std::this_thread::sleep_for(std::chrono::milliseconds(50));
               if (_allocator.total_pending_releases() == 0)
               {
                  // Publish any accumulated dead versions so defrag/COW can see them
                  _dead_versions.flush_pending();
                  _dead_versions.publish_snapshot();
                  return true;
               }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
         }
         return false;
      }

      void compact_and_truncate()
      {
         wait_for_compactor();
         _allocator.truncate_free_tail();
      }

      void defrag();

      ///@}

      /** @name Recovery */
      ///@{

      bool ref_counts_stale() const;

      void reclaim_leaked_memory();

      sal::verify_result verify() { return detail::verify_all_roots(_allocator); }

      version_audit_result audit_versions(version_audit_options options = {})
      {
         return detail::audit_all_roots(_allocator, options);
      }

      version_index_audit_result audit_version_index();

      tree_stats_result tree_stats(tree_stats_options options = {});

      void recover() { _allocator.recover(); }

      void reset_reference_counts() { _allocator.reset_reference_counts(); }

      ///@}

      /** @name Low-Level Diagnostics */
      ///@{

      sal::seg_alloc_dump dump() const { return _allocator.dump(); }

      uint64_t reachable_size() { return _allocator.reachable_size(); }

      auto audit_freed_space() { return _allocator.audit_freed_space(); }

      ///@}

      /** @name MVCC Version Reclamation */
      ///@{

      /// Access the dead-version range map (for diagnostics or testing).
      live_range_map& dead_versions() noexcept { return _dead_versions; }
      const live_range_map& dead_versions() const noexcept { return _dead_versions; }

      ///@}

      /// @cond INTERNAL
      mutex_type& modify_lock(int index) { return _modify_lock[index]; }

      /// Internal accessor used by layered components (e.g. DWAL) to
      /// perform ref-count manipulation on addresses without going
      /// through a thread-affine session. `sal::allocator::retain` is
      /// atomic; `sal::allocator::release` forwards to the calling
      /// thread's thread-local session, so both can be called from any
      /// thread safely.
      sal::allocator& underlying_allocator() noexcept { return _allocator; }
      /// @endcond

     private:
      template <class> friend class basic_read_session;
      template <class> friend class basic_write_session;

      std::filesystem::path _dir;
      runtime_config        _cfg;

      mutable mutex_type _sync_mutex;
      /// Lightweight lock for root slot version-CB swaps (fast-path MVCC).
      /// Does NOT protect tree structure — only the ver field in the root slot.
      mutable mutex_type _root_ver_mutex[num_top_roots];
      /// Full per-root mutex for COW mutations that change the root.
      mutable mutex_type _modify_lock[num_top_roots];

      mutex_type& root_ver_lock(int index) { return _root_ver_mutex[index]; }

      // ── Stripe locks for fine-grained MVCC concurrency ──────────
      static constexpr uint32_t num_stripe_locks = 1024;

      struct alignas(64) stripe_lock
      {
         mutex_type m;
      };
      stripe_lock _stripes[num_stripe_locks];

      uint32_t stripe_index(sal::ptr_address adr) const noexcept
      {
         uint32_t v = *adr;
         return XXH3_64bits(&v, sizeof(v)) & (num_stripe_locks - 1);
      }

      mutex_type& stripe_mutex(sal::ptr_address adr) { return _stripes[stripe_index(adr)].m; }

      void maybe_publish_dead_versions_for_epoch(uint64_t epoch_base)
      {
         const uint64_t epoch_token = version_token(epoch_base, last_unique_version_bits);
         if (epoch_token == 0)
            return;

         std::lock_guard<mutex_type> lock(_dead_publish_mutex);
         if (!version_newer_than(epoch_token, _last_dead_publish_epoch,
                                 last_unique_version_bits))
            return;

         _dead_versions.flush_pending();
         _dead_versions.publish_snapshot();
         _last_dead_publish_epoch = epoch_token;
      }

	      void           init_allocator_shared_ownership();
	      void           recover_global_version_from_roots();
	      void           bootstrap_dead_versions_from_control_blocks();
	      std::once_flag _alloc_shared_init;
      live_range_map _dead_versions;
      mutable mutex_type _dead_publish_mutex;
      uint64_t           _last_dead_publish_epoch = 0;
      detail::psitri_object_registry _object_registry;
      sal::allocator _allocator;
      sal::mapping            _dbfile;
      detail::database_state* _dbm;
   };

   /// Default-policy alias preserved for existing consumers.
   using database     = basic_database<std_lock_policy>;
   using database_ptr = std::shared_ptr<database>;

}  // namespace psitri
