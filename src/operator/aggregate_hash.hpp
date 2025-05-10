//
// Created by lilac on 25-5-8.
//

#ifndef AGGREGATE_HASH_HPP
#define AGGREGATE_HASH_HPP
#include <bits/stdc++.h>
#include "../storage/storage.hpp"
#include "../types.hpp"

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
                const std::vector<ColumnID>& _groupby_column_ids){
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
          } else {
            /*
            Store unique IDs for equal values in the groupby column (similar to dictionary encoding).
            The ID 0 is reserved for NULL values. The combined IDs build an AggregateKey for each row.
            */

            // This time, we have no idea how much space we need, so we take some memory and then rely on the automatic
            // resizing. The size is quite random, but since single memory allocations do not cost too much, we rather
            // allocate a bit too much.
            auto temp_buffer = std::pmr::monotonic_buffer_resource(1'000'000);
            auto allocator = std::pmr::polymorphic_allocator<std::pair<const ColumnDataType, AggregateKeyEntry>>{&temp_buffer};

            // 使用std::unordered_map替代boost::unordered_flat_map
            auto id_map = std::unordered_map<ColumnDataType, AggregateKeyEntry, std::hash<ColumnDataType>,
                                           std::equal_to<>, decltype(allocator)>(allocator);
            auto id_counter = AggregateKeyEntry{1};

            if constexpr (std::is_same_v<ColumnDataType, std::string>) {
              // We store strings shorter than five characters without using the id_map. For that, we need to reserve
              // the IDs used for short strings (see below).
              id_counter = 5'000'000'000;
            }

            for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
              const auto chunk_in = input_table->get_chunk(chunk_id);
              if (!chunk_in) {
                continue;
              }

              auto& keys = keys_per_chunk[chunk_id];

              const auto abstract_segment = chunk_in->get_segment(groupby_column_id);
              auto chunk_offset = ChunkOffset{0};
              segment_iterate<ColumnDataType>(*abstract_segment, [&](const auto& position) {
                if (position.is_null()) {
                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = 0u;
                  } else {
                    keys[chunk_offset][group_column_index] = 0u;
                  }
                } else {
                  // We need to generate an ID that is unique for the value. In some cases, we can use an optimization,
                  // in others, we cannot. We need to somehow track whether we have found an ID or not. For this, we
                  // first set `value_id` to its maximum value. If after all branches it is still that max value, no
                  // optimized  ID generation was applied and we need to generate the ID using the value->ID map.
                  auto value_id = std::numeric_limits<AggregateKeyEntry>::max();

                  if constexpr (std::is_same_v<ColumnDataType, std::string>) {
                    const auto& string = position.value();
                    if (string.size() < 5) {
                      static_assert(std::is_same_v<AggregateKeyEntry, uint64_t>, "Calculation only valid for uint64_t");

                      const auto char_to_uint = [](const char char_in, const uint32_t bits) {
                        // chars may be signed or unsigned. For the calculation as described below, we need signed
                        // chars.
                        return static_cast<uint64_t>(*reinterpret_cast<const uint8_t*>(&char_in)) << bits;
                      };

                      switch (string.size()) {
                          // Optimization for short strings (see above):
                          //
                          // NULL:              0
                          // str.length() == 0: 1
                          // str.length() == 1: 2 + (uint8_t) str            // maximum: 257 (2 + 0xff)
                          // str.length() == 2: 258 + (uint16_t) str         // maximum: 65'793 (258 + 0xffff)
                          // str.length() == 3: 65'794 + (uint24_t) str      // maximum: 16'843'009
                          // str.length() == 4: 16'843'010 + (uint32_t) str  // maximum: 4'311'810'305
                          // str.length() >= 5: map-based identifiers, starting at 5'000'000'000 for better distinction
                          //
                          // This could be extended to longer strings if the size of the input table (and thus the
                          // maximum number of distinct strings) is taken into account. For now, let's not make it even
                          // more complicated.

                        case 0: {
                          value_id = uint64_t{1};
                        } break;

                        case 1: {
                          value_id = uint64_t{2} + char_to_uint(string[0], 0);
                        } break;

                        case 2: {
                          value_id = uint64_t{258} + char_to_uint(string[1], 8) + char_to_uint(string[0], 0);
                        } break;

                        case 3: {
                          value_id = uint64_t{65'794} + char_to_uint(string[2], 16) + char_to_uint(string[1], 8) +
                                     char_to_uint(string[0], 0);
                        } break;

                        case 4: {
                          value_id = uint64_t{16'843'010} + char_to_uint(string[3], 24) + char_to_uint(string[2], 16) +
                                     char_to_uint(string[1], 8) + char_to_uint(string[0], 0);
                        } break;
                      }
                    }
                  }

                  if (value_id == std::numeric_limits<AggregateKeyEntry>::max()) {
                    // Could not take the shortcut above, either because we don't have a string or because it is too
                    // long.
                    auto inserted = id_map.try_emplace(position.value(), id_counter);

                    value_id = inserted.first->second;

                    // If the id_map did not have the value as a key and a new element was inserted.
                    if (inserted.second) {
                      ++id_counter;
                    }
                  }

                  if constexpr (std::is_same_v<AggregateKey, AggregateKeyEntry>) {
                    keys[chunk_offset] = value_id;
                  } else {
                    keys[chunk_offset][group_column_index] = value_id;
                  }
                }

                ++chunk_offset;
              });
            }

            // We will see at least `id_map.size()` different groups. We can use this knowledge to preallocate memory
            // for the results. Estimating the number of groups for multiple GROUP BY columns is somewhat hard, so we
            // simply take the number of groups created by the GROUP BY column with the highest number of distinct
            // values.
            auto previous_max = _expected_result_size.load();
            while (previous_max < id_map.size()) {
              // _expected_result_size needs to be atomatically updated as the GROUP BY columns are processed in
              // parallel. How to atomically update a maximum value? from https://stackoverflow.com/a/16190791/2204581
              if (_expected_result_size.compare_exchange_strong(previous_max, id_map.size())) {
                break;
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
  std::cerr << "partition_by_groupby_keys time: " << duration << "s" << std::endl;
  return keys_per_chunk;
}

template <typename AggregateKey>
void _aggregate(const std::shared_ptr<Table>& input_table,
                const std::vector<std::string>& aggregates,
                const std::vector<ColumnID>& _groupby_column_ids){
    auto keys_per_chunk = _partition_by_groupby_keys<AggregateKey>(input_table, aggregates, _groupby_column_ids);
}

std::shared_ptr<const Table> aggregate_hash(const std::shared_ptr<Table>& input_table,
                const std::vector<std::string>& aggregates,
                const std::vector<ColumnID>& _groupby_column_ids) {
    switch (_groupby_column_ids.size()) {
        case 0:
//            _aggregate<EmptyAggregateKey>();
        break;
        case 1:
            // No need for a complex data structure if we only have one entry.
            // 默认group key为1
            _aggregate<AggregateKeyEntry>(input_table, aggregates, _groupby_column_ids);
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
