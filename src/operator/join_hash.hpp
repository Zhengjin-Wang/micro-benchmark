//
// Created by lilac on 25-4-10.
//

#ifndef JOIN_HASH_HPP
#define JOIN_HASH_HPP
#include <bits/stdc++.h>
#include "../storage/segment.hpp"
#include "../storage/storage.hpp"
#define DEBUG 1

// #define BOOST_BITSET
static constexpr auto JOB_SPAWN_THRESHOLD = 500;

template <typename T>
struct PartitionedElement {
    PartitionedElement() = default;
    PartitionedElement(RowID row_id, T value) : row_id(row_id), value(value) {}

    RowID row_id;
    T value;
};

template <typename T>
struct Partition {
    std::vector<PartitionedElement<T>> elements;
    std::vector<bool> null_values;
};

template <typename T>
using RadixContainer = std::vector<Partition<T>>;

static constexpr auto BLOOM_FILTER_SIZE = 1 << 20;
static constexpr auto BLOOM_FILTER_MASK = BLOOM_FILTER_SIZE - 1;
using Hash = size_t;

#ifndef BOOST_BITSET
using BloomFilter = std::bitset<BLOOM_FILTER_SIZE>;
static const auto ALL_TRUE_BLOOM_FILTER = BloomFilter{}.set();
#else
#include <boost/dynamic_bitset.hpp>
using BloomFilter = boost::dynamic_bitset<>;
static const auto ALL_TRUE_BLOOM_FILTER = ~BloomFilter(BLOOM_FILTER_SIZE);
#endif

template <typename T, typename HashedType>
RadixContainer<T> materialize_input(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                    std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                    BloomFilter& output_bloom_filter,
                                    const BloomFilter& input_bloom_filter, bool output_time) {
#ifdef BOOST_BITSET
    output_bloom_filter.resize(BLOOM_FILTER_SIZE);
#endif
    const auto start = std::chrono::high_resolution_clock::now();
    const auto chunk_count = in_table->chunk_count();

    const std::hash<HashedType> hash_function;

    // histograms resize
    histograms.resize(chunk_count);

    const size_t num_radix_partitions = 1ull << radix_bits;

    const auto pass = size_t{0};
    const auto radix_mask = static_cast<size_t>(std::pow(2, radix_bits * (pass + 1)) - 1);

    // radix_container copy
    auto radix_container = RadixContainer<T>{};
    radix_container.resize(chunk_count);
    auto materialize = [&](uint32_t chunk_id) {
        auto chunk_in = in_table->get_chunk(chunk_id);
        auto base_segment = chunk_in->get_segment(column_id);
        auto segment = std::dynamic_pointer_cast<IntSegment>(base_segment);
        // auto segment = base_segment;
        const auto num_rows = chunk_in->size();

        auto& elements = radix_container[chunk_id].elements;
        auto& null_values = radix_container[chunk_id].null_values;

        // std::cerr << "Number of rows in one chunk: " << num_rows << std::endl;

        elements.resize(num_rows);
        null_values.resize(num_rows);

        auto histogram = std::vector<size_t>(num_radix_partitions);
        int element_idx = 0;
        for (uint32_t i = 0; i < num_rows; ++i) {
            auto& value = segment->at(i);
            auto& actual_value = value.value();
            // auto& actual_value = std::get<T>(value.value());
            const auto hashed_value = hash_function(static_cast<HashedType>(actual_value));
            auto skip = false;
            if (!value.is_null() && !input_bloom_filter[hashed_value & BLOOM_FILTER_MASK]) {
                // Value in not present in input bloom filter and can be skipped
                skip = true;
            }

            if(!skip) {
                output_bloom_filter.set(hashed_value & BLOOM_FILTER_MASK);

                elements[element_idx] = PartitionedElement<T>{RowID{chunk_id, value.chunk_offset()}, actual_value};
                null_values[element_idx] = value.is_null();
                ++element_idx;

                if (radix_bits > 0) {
                    const Hash radix = hashed_value & radix_mask;
                    ++histogram[radix];
                }
            }
        }
        elements.resize(element_idx);
        null_values.resize(element_idx);

        histograms[chunk_id] = std::move(histogram);
    };
    // copy memory
    std::vector<std::future<void>> futures;
    for (uint32_t chunk_id = 0; chunk_id < chunk_count; ++chunk_id){
        futures.emplace_back(std::async(std::launch::async, materialize, chunk_id));
    }

    // 等待所有线程完成
    for (auto& future : futures) {
        future.get();
    }

    if (DEBUG) {
        // for (auto& partition:radix_container) {
        //     for (auto& element : partition.elements) {
        //         std::cout << element.value << " ";
        //     }
        //     std::cout << std::endl;
        // }
    }

    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration<double>(end - start).count();
    if (output_time)
    {
        std::cerr << "materialize_input time: " << duration << "s" << std::endl;
    }

    return radix_container;

}

template <typename T, typename HashedType, bool keep_null_values>
RadixContainer<T> partition_by_radix(const RadixContainer<T>& radix_container,
                                     std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                     const BloomFilter& input_bloom_filter = ALL_TRUE_BLOOM_FILTER, bool output_time = false) {

    if (radix_container.empty()) {
    return radix_container;
  }
    auto start = std::chrono::high_resolution_clock::now();
  if constexpr (keep_null_values) {
    Assert(radix_container[0].elements.size() == radix_container[0].null_values.size(),
           "partition_by_radix() called with NULL consideration but radix container does not store any NULL "
           "value information");
  }

  const std::hash<HashedType> hash_function;

  const auto input_partition_count = radix_container.size();
  const auto output_partition_count = size_t{1} << radix_bits;

  // currently, we just do one pass
  const size_t pass = 0;
  const size_t radix_mask = static_cast<uint32_t>(std::pow(2, radix_bits * (pass + 1)) - 1);

  // allocate new (shared) output
  auto output = RadixContainer<T>(output_partition_count);

  Assert(histograms.size() == input_partition_count, "Expected one histogram per input partition");
  Assert(histograms[0].size() == output_partition_count, "Expected one histogram bucket per output partition");

  // Writing to std::vector<bool> is not thread-safe if the same byte is being written to. For now, we temporarily
  // use a std::vector<char> and compress it into an std::vector<bool> later.
  auto null_values_as_char = std::vector<std::vector<char>>(output_partition_count);

  // output_offsets_by_input_partition[input_partition_idx][output_partition_idx] holds the first offset in the
  // bucket written for input_partition_idx
  auto output_offsets_by_input_partition =
      std::vector<std::vector<size_t>>(input_partition_count, std::vector<size_t>(output_partition_count));
  for (auto output_partition_idx = size_t{0}; output_partition_idx < output_partition_count; ++output_partition_idx) {
    auto this_output_partition_size = size_t{0};
    for (auto input_partition_idx = size_t{0}; input_partition_idx < input_partition_count; ++input_partition_idx) {
      output_offsets_by_input_partition[input_partition_idx][output_partition_idx] = this_output_partition_size;
      this_output_partition_size += histograms[input_partition_idx][output_partition_idx];
    }

    output[output_partition_idx].elements.resize(this_output_partition_size);

  }

    std::vector<std::future<void>> jobs;
  jobs.reserve(input_partition_count);

  for (auto input_partition_idx = ChunkID{0}; input_partition_idx < input_partition_count; ++input_partition_idx) {
    const auto& input_partition = radix_container[input_partition_idx];
    const auto& elements = input_partition.elements;
    const auto elements_count = elements.size();

    const auto perform_partition = [&, input_partition_idx, elements_count]() {
      for (auto input_idx = size_t{0}; input_idx < elements_count; ++input_idx) {
        const auto& element = elements[input_idx];

        const size_t radix = hash_function(static_cast<HashedType>(element.value)) & radix_mask;

        auto& output_idx = output_offsets_by_input_partition[input_partition_idx][radix];

          output[radix].elements[output_idx] = element;

        ++output_idx;
      }
    };
    if (JOB_SPAWN_THRESHOLD > elements_count) {
      perform_partition();
    } else {
      jobs.emplace_back(std::async(std::launch::async, perform_partition));
    }
  }
    for (auto& future : jobs) {
        future.get();
    }
  jobs.clear();

  // Compress null_values_as_char into partition.null_values
  if constexpr (keep_null_values) {
    for (auto output_partition_idx = size_t{0}; output_partition_idx < output_partition_count; ++output_partition_idx) {
      jobs.emplace_back(std::async(std::launch::async, [&, output_partition_idx]() {
        const auto null_value_count = output[output_partition_idx].null_values.size();
        for (auto element_idx = size_t{0}; element_idx < null_value_count; ++element_idx) {
          output[output_partition_idx].null_values[element_idx] =
              null_values_as_char[output_partition_idx][element_idx];
        }
      }));
    }
      for (auto& future : jobs) {
          future.get();
      }
  }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    if (output_time)
    {
        std::cerr << "partition_by_radix time: " << duration << "s" << std::endl;
    }
  return output;
}

// keep nulls is false
template <typename BuildColumnType, typename ProbeColumnType, typename HashedType>
void join_hash(std::shared_ptr<const Table> build_input_table, std::shared_ptr<const Table> probe_input_table,
               std::pair<ColumnID, ColumnID>& column_ids, const size_t radix_bits, const std::string& test_op) {

    auto keep_nulls_build_column = false;
    auto keep_nulls_probe_column = false;

    auto histograms_build_column = std::vector<std::vector<size_t>>{};
    auto histograms_probe_column = std::vector<std::vector<size_t>>{};

    auto materialized_build_column = RadixContainer<BuildColumnType>{};
    auto materialized_probe_column = RadixContainer<ProbeColumnType>{};

    auto radix_build_column = RadixContainer<BuildColumnType>{};
    auto radix_probe_column = RadixContainer<ProbeColumnType>{};

    auto build_side_bloom_filter = BloomFilter{};
    auto probe_side_bloom_filter = BloomFilter{};

    auto output_materialize_input_time = false;
    auto output_partition_by_radix_time = false;
    if (test_op == "join_hash")
    {
        output_materialize_input_time = true;
        output_partition_by_radix_time = true;
    }
    else if (test_op == "materialize_input")
    {
        output_materialize_input_time = true;
    }
    else if (test_op == "partition_by_radix")
    {
        output_partition_by_radix_time = true;
    }

    const auto materialize_build_side = [&](const auto& input_bloom_filter) {
            materialized_build_column = materialize_input<BuildColumnType, HashedType>(
                build_input_table, column_ids.first, histograms_build_column, radix_bits, build_side_bloom_filter,
                input_bloom_filter, output_materialize_input_time);
    };

    const auto materialize_probe_side = [&](const auto& input_bloom_filter) {
            materialized_probe_column = materialize_input<ProbeColumnType, HashedType>(
                probe_input_table, column_ids.second, histograms_probe_column, radix_bits, probe_side_bloom_filter,
                input_bloom_filter, output_materialize_input_time);
    };

    if (build_input_table->row_count() < probe_input_table->row_count()) {
        // When materializing the first side (here: the build side), we do not yet have a Bloom filter. To keep the number
        // of code paths low, materialize_*_side always expects a Bloom filter. For the first step, we thus pass in a
        // Bloom filter that returns true for every probe.
        materialize_build_side(ALL_TRUE_BLOOM_FILTER);
        materialize_probe_side(build_side_bloom_filter);
    } else {
        // Here, we first materialize the probe side and use the resulting Bloom filter in the materialization of the
        // build side. Consequently, the Bloom filter later passed into build() will have no effect as it has already
        // been used here to filter non-matching values.
        materialize_probe_side(ALL_TRUE_BLOOM_FILTER);
        materialize_build_side(probe_side_bloom_filter);
    }

    if (radix_bits > 0) {
        std::vector<std::future<void>> jobs;

        jobs.emplace_back(std::async(std::launch::deferred,[&]() {
          // radix partition the build table
          if (keep_nulls_build_column) {
            radix_build_column = partition_by_radix<BuildColumnType, HashedType, true>(
                materialized_build_column, histograms_build_column, radix_bits, ALL_TRUE_BLOOM_FILTER, output_partition_by_radix_time);
          } else {
            radix_build_column = partition_by_radix<BuildColumnType, HashedType, false>(
                materialized_build_column, histograms_build_column, radix_bits, ALL_TRUE_BLOOM_FILTER, output_partition_by_radix_time);
          }

          // After the data in materialized_build_column has been partitioned, it is not needed anymore.
          materialized_build_column.clear();
        }));

        jobs.emplace_back(std::async(std::launch::deferred,[&]() {
          // radix partition the probe column.
          if (keep_nulls_probe_column) {
            radix_probe_column = partition_by_radix<ProbeColumnType, HashedType, true>(
                materialized_probe_column, histograms_probe_column, radix_bits, ALL_TRUE_BLOOM_FILTER, output_partition_by_radix_time);
          } else {
            radix_probe_column = partition_by_radix<ProbeColumnType, HashedType, false>(
                materialized_probe_column, histograms_probe_column, radix_bits, ALL_TRUE_BLOOM_FILTER, output_partition_by_radix_time);
          }

          // After the data in materialized_probe_column has been partitioned, it is not needed anymore.
          materialized_probe_column.clear();
        }));

        for (auto& future : jobs) {
            future.get();
        }
        histograms_build_column.clear();
        histograms_probe_column.clear();

    } else {
        // short cut: skip radix partitioning and use materialized data directly
        radix_build_column = std::move(materialized_build_column);
        radix_probe_column = std::move(materialized_probe_column);
    }
}
#endif //JOIN_HASH_HPP
