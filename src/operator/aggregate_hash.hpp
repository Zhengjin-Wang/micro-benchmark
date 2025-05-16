//
// Created by lilac on 25-5-8.
//

#ifndef AGGREGATE_HASH_HPP
#define AGGREGATE_HASH_HPP
#include <bits/stdc++.h>
#include "../storage/storage.hpp"
#include "../types.hpp"
#include "aggregate_segment.hpp"

struct EmptyAggregateKey {};
using AggregateKeyEntry = uint64_t;
using AggregateKeySmallVector = std::vector<AggregateKeyEntry>;

constexpr auto CACHE_MASK = AggregateKeyEntry{1} << 63u;

template <typename AggregateKey>
using AggregateKeys =
    std::conditional_t<std::is_same_v<AggregateKey, AggregateKeySmallVector>, std::vector<AggregateKey>,std::vector<AggregateKey>>;

template <typename AggregateKey>
using KeysPerChunk = std::vector<AggregateKeys<AggregateKey>>;

template <typename AggregateKey>
KeysPerChunk<AggregateKey> _partition_by_groupby_keys(const std::shared_ptr<Table>& input_table,
                const std::vector<std::string>& aggregates,
                const std::vector<ColumnID>& _groupby_column_ids,
                bool output_time){
  const auto start = std::chrono::high_resolution_clock::now();
  auto keys_per_chunk = KeysPerChunk<AggregateKey>{};

  if constexpr (!std::is_same_v<AggregateKey, EmptyAggregateKey>) {
    const auto chunk_count = input_table->chunk_count();

    // Create the actual data structure
    keys_per_chunk.reserve(chunk_count);
    for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
      const auto chunk = input_table->get_chunk(chunk_id);
      if (!chunk) {
        continue;
      }

      if constexpr (std::is_same_v<AggregateKey, AggregateKeySmallVector>) {
        keys_per_chunk.emplace_back(chunk->size(), AggregateKey(_groupby_column_ids.size()));
      } else {
        keys_per_chunk.emplace_back(chunk->size(), AggregateKey{});
      }
    }

    // Now that we have the data structures in place, we can start the actual work. We want to fill
    // keys_per_chunk[chunk_id][chunk_offset] with something that uniquely identifies the group into which that
    // position belongs. There are a couple of options here (cf. AggregateHash::_on_execute):
    //
    // 0 GROUP BY columns:   No partitioning needed; we do not reach this point because of the check for
    //                       EmptyAggregateKey above
    // 1 GROUP BY column:    The AggregateKey is one dimensional, i.e., the same as AggregateKeyEntry
    // > 1 GROUP BY columns: The AggregateKey is multi-dimensional. The value in
    //                       keys_per_chunk[chunk_id][chunk_offset] is subscripted with the index of the GROUP BY
    //                       columns (not the same as the GROUP BY column_id)
    //
    // To generate a unique identifier, we create a map from the value found in the respective GROUP BY column to a
    // unique uint64_t. The value 0 is reserved for NULL.
    //
    // This has the cost of a hashmap lookup and potential insert for each row and each GROUP BY column. There are some
    // cases in which we can avoid this. These make use of the fact that we can only have 2^64 - 2*2^32 values in a
    // table (due to INVALID_VALUE_ID and INVALID_CHUNK_OFFSET limiting the range of RowIDs).
    //
    // (1) For types smaller than AggregateKeyEntry, such as int32_t, their value range can be immediately mapped into
    //     uint64_t. We cannot do the same for int64_t because we need to account for NULL values.
    // (2) For strings not longer than five characters, there are 1+2^(1*8)+2^(2*8)+2^(3*8)+2^(4*8) potential values.
    //     We can immediately map these into a numerical representation by reinterpreting their byte storage as an
    //     integer. The calculation is described below. Note that this is done on a per-string basis and does not
    //     require all strings in the given column to be that short.
    std::vector<std::future<void>> jobs;
    jobs.reserve(_groupby_column_ids.size());

    const auto groupby_column_count = _groupby_column_ids.size();
    for (auto group_column_index = size_t{0}; group_column_index < groupby_column_count; ++group_column_index) {
      jobs.emplace_back(std::async(std::launch::async,([&input_table, group_column_index, &keys_per_chunk, chunk_count, &_groupby_column_ids]() {
        std::atomic_size_t _expected_result_size{};
        const auto groupby_column_id = _groupby_column_ids.at(group_column_index);
        const auto type = input_table->column_data_type(groupby_column_id);

        {
          using ColumnDataType = int;

          if constexpr (std::is_same_v<ColumnDataType, int>) {
            // For values with a smaller type than AggregateKeyEntry, we can use the value itself as an
            // AggregateKeyEntry. We cannot do this for types with the same size as AggregateKeyEntry as we need to have
            // a special NULL value. By using the value itself, we can save us the effort of building the id_map.

            // Track the minimum and maximum key for the immediate key optimization. Search this cpp file for the last
            // use of `min_key` for a longer explanation.
            auto min_key = std::numeric_limits<AggregateKeyEntry>::max();
            auto max_key = uint64_t{0};

            for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto chunk_in = input_table->get_chunk(chunk_id);
              const auto segment = std::dynamic_pointer_cast<IntSegment>(chunk_in->get_segment(groupby_column_id));
              ChunkOffset chunk_offset{0};
              auto& keys = keys_per_chunk[chunk_id];
              auto segment_size = segment->size();
              for (ChunkOffset i = 0; i < segment_size; ++i) {
               auto position = segment->at(i);
                const auto int_to_uint = [](const int32_t value) {
                  // We need to convert a potentially negative int32_t value into the uint64_t space. We do not care
                  // about preserving the value, just its uniqueness. Subtract the minimum value in int32_t (which is
                  // negative itself) to get a positive number.
                  const auto shifted_value = static_cast<int64_t>(value) - std::numeric_limits<int32_t>::min();
//                  DebugAssert(shifted_value >= 0, "Type conversion failed");
                  return static_cast<uint64_t>(shifted_value);
                };

                if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                  // Single GROUP BY column
                  if (position.is_null()) {
                    keys[chunk_offset] = 0;
                  } else {
                    const auto key = int_to_uint(position.value()) + 1;

                    keys[chunk_offset] = key;

                    min_key = std::min(min_key, key);
                    max_key = std::max(max_key, key);
                  }
                } else {
                  // Multiple GROUP BY columns
                  if (position.is_null()) {
                    keys[chunk_offset][group_column_index] = 0;
                  } else {
                    keys[chunk_offset][group_column_index] = int_to_uint(position.value()) + 1;
                  }
                }
                ++chunk_offset;
              }
            }

            if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
              // In some cases (e.g., TPC-H Q18), we aggregate with consecutive int32_t values being used as a GROUP BY
              // key. Notably, this is the case when aggregating on the serial primary key of a table without filtering
              // the table before. In these cases, we do not need to perform a full hash-based aggregation, but can use
              // the values as immediate indexes into the list of results. To handle smaller gaps, we include cases up
              // to a certain threshold, but at some point these gaps make the approach less beneficial than a proper
              // hash-based approach. Both min_key and max_key do not correspond to the original int32_t value, but are
              // the result of the int_to_uint transformation. As such, they are guaranteed to be positive. This
              // shortcut only works if we are aggregating with a single GROUP BY column (i.e., when we use
              // AggregateKeyEntry) - otherwise, we cannot establish a 1:1 mapping from keys_per_chunk to the result id.
              // TODO(anyone): Find a reasonable threshold.
              if (max_key > 0 &&
                  static_cast<double>(max_key - min_key) < static_cast<double>(input_table->row_count()) * 1.2) {
                // Include space for min, max, and NULL
                _expected_result_size = static_cast<size_t>(max_key - min_key) + 2;
                auto _use_immediate_key_shortcut = true;

                // Rewrite the keys and (1) subtract min so that we can also handle consecutive keys that do not start
                // at 1* and (2) set the first bit which indicates that the key is an immediate index into the result
                // vector (see get_or_add_result).
                // *) Note: Because of int_to_uint above, the values do not start at 1, anyway.

                for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
                  const auto chunk_size = input_table->get_chunk(chunk_id)->size();
                  for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
                    auto& key = keys_per_chunk[chunk_id][chunk_offset];
                    if (key == 0) {
                      // Key that denotes NULL, do not rewrite but set the cached flag
                      key = key | CACHE_MASK;
                    } else {
                      key = (key - min_key + 1) | CACHE_MASK;
                    }
                  }
                }
              }
            }
          }
        }
      })));
    }

    for (auto& job : jobs) {
      job.get();
    }
  }
  const auto end = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration<double>(end - start).count();
  if(output_time) {
    std::cerr << "partition_by_groupby_keys time: " << duration << "s" << std::endl;
  }
  return keys_per_chunk;
}

template <typename AggregateKey>
void _aggregate(const std::shared_ptr<Table>& input_table,
                const std::vector<std::string>& aggregates,
                const std::vector<ColumnID>& _groupby_column_ids,
                const std::string& test_op){

    auto output_partition_by_groupby_keys_time = false;
    auto output_aggregate_segment_time = false;
   if(test_op == "aggregate_hash") {
     output_partition_by_groupby_keys_time = true;
     output_aggregate_segment_time = true;
   }
    else if (test_op == "partition_by_groupby_keys") {
      output_partition_by_groupby_keys_time = true;
    }
    else if (test_op == "aggregate_segment") {
      output_aggregate_segment_time = true;
    }

    auto keys_per_chunk = _partition_by_groupby_keys<AggregateKey>(input_table, aggregates, _groupby_column_ids, output_partition_by_groupby_keys_time);

  const auto start = std::chrono::high_resolution_clock::now();
  auto chunk_count = input_table->chunk_count();
  for(auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
    auto chunk = input_table->get_chunk(chunk_id);
    auto column_count = chunk->column_count();
    for (auto column_id = ColumnID{0}; chunk_id < column_count; ++chunk_id) {
      auto base_segment = chunk->get_segment(column_id);
      auto& type = chunk->column_defs()[column_id].type;
      if(std::holds_alternative<int>(type)) {
        auto segment = std::dynamic_pointer_cast<IntSegment>(base_segment);
        // 创建上下文
        auto context = std::make_shared<AggregateContext<AggregateKey>>();

        // 创建聚合器
        auto aggregator = std::make_shared<SumAggregator<int, AggregateKey>>();
        for(auto& s:aggregates) {
          if(s == "sum") {
            aggregator = std::make_shared<SumAggregator<int, AggregateKey>>();
          }
        }
        aggregate_segment<int, AggregateKey>(
              chunk_id,
              segment->values(),
              segment->null_values(),
              keys_per_chunk[chunk_id],
              context,
              aggregator
          );
      }
    }
  }
  const auto end = std::chrono::high_resolution_clock::now();
  const auto duration = std::chrono::duration<double>(end - start).count();
  if(output_aggregate_segment_time) {
    std::cerr << "aggregate_segment time: " << duration << "s" << std::endl;
  }
}

std::shared_ptr<const Table> aggregate_hash(const std::shared_ptr<Table>& input_table,
                const std::vector<std::string>& aggregates,
                const std::vector<ColumnID>& _groupby_column_ids,
                const std::string& test_op) {
    switch (_groupby_column_ids.size()) {
        case 0:
//            _aggregate<EmptyAggregateKey>();
        break;
        case 1:
            // No need for a complex data structure if we only have one entry.
            // 默认group key为1
            _aggregate<AggregateKeyEntry>(input_table, aggregates, _groupby_column_ids, test_op);
        break;
        case 2:
//            _aggregate<std::array<AggregateKeyEntry, 2>>();
        break;
        default:
//            _aggregate<AggregateKeySmallVector>();
        break;
    }

  return nullptr;
}

#endif //AGGREGATE_HASH_HPP
