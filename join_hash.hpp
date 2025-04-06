//
// Created by lilac on 25-4-6.
//
#include <bits/stdc++.h>
#include "segment.hpp"
#include "storage.hpp"
#define DEBUG 1

#ifndef JOIN_HASH_STEPS_HPP
#define JOIN_HASH_STEPS_HPP

class RowID {
public:
    RowID() = default;
    RowID(int32_t chunk_id, int32_t chunk_offset) : chunk_id(chunk_id), chunk_offset(chunk_offset) {}

    int32_t chunk_id{0};
    int32_t chunk_offset{0};
};

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

using BloomFilter = std::bitset<BLOOM_FILTER_SIZE>;

static const auto ALL_TRUE_BLOOM_FILTER = BloomFilter{}.set();

template <typename T, typename HashedType>
RadixContainer<T> materialize_input(const std::shared_ptr<const Table>& in_table, const ColumnID column_id,
                                    std::vector<std::vector<size_t>>& histograms, const size_t radix_bits,
                                    BloomFilter& output_bloom_filter,
                                    const BloomFilter& input_bloom_filter) {
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

    // copy memory
    for (int32_t chunk_id = 0; chunk_id < chunk_count; ++chunk_id){
        auto chunk_in = in_table->get_chunk(chunk_id);
        auto segment = chunk_in->get_segment(column_id);
        const auto num_rows = chunk_in->row_count();

        auto& elements = radix_container[chunk_id].elements;
        auto& null_values = radix_container[chunk_id].null_values;

        // std::cerr << "Number of rows in one chunk: " << num_rows << std::endl;

        elements.resize(num_rows);
        null_values.resize(num_rows);

        auto histogram = std::vector<size_t>(num_radix_partitions);

        for (size_t i = 0; i < num_rows; ++i) {
            auto& value = segment->at(i);
            auto& actual_value = std::get<T>(value.value());
            const auto hashed_value = hash_function(static_cast<HashedType>(actual_value));
            auto skip = false;
            if (!value.is_null() && !input_bloom_filter[hashed_value & BLOOM_FILTER_MASK]) {
                // Value in not present in input bloom filter and can be skipped
                skip = true;
            }

            if(!skip) {
                output_bloom_filter.set(hashed_value & BLOOM_FILTER_MASK);

                elements[i] = PartitionedElement<T>{RowID{chunk_id, value.chunk_offset()}, actual_value};
                null_values[i] = value.is_null();

                if (radix_bits > 0) {
                    const Hash radix = hashed_value & radix_mask;
                    ++histogram[radix];
                }
            }
        }

        histograms[chunk_id] = std::move(histogram);

    }
    if (DEBUG) {
        // for (auto& partition:radix_container) {
        //     for (auto& element : partition.elements) {
        //         std::cout << element.value << " ";
        //     }
        //     std::cout << std::endl;
        // }
    }

    return radix_container;

}


template <typename BuildColumnType, typename ProbeColumnType, typename HashedType>
void join_hash(std::shared_ptr<const Table> build_input_table, std::shared_ptr<const Table> probe_input_table,
               std::pair<ColumnID, ColumnID>& column_ids, const size_t radix_bits) {

    auto histograms_build_column = std::vector<std::vector<size_t>>{};
    auto histograms_probe_column = std::vector<std::vector<size_t>>{};

    auto materialized_build_column = RadixContainer<BuildColumnType>{};
    auto materialized_probe_column = RadixContainer<ProbeColumnType>{};

    auto radix_build_column = RadixContainer<BuildColumnType>{};
    auto radix_probe_column = RadixContainer<ProbeColumnType>{};

    auto build_side_bloom_filter = BloomFilter{};
    auto probe_side_bloom_filter = BloomFilter{};

    const auto materialize_build_side = [&](const auto& input_bloom_filter) {
            materialized_build_column = materialize_input<BuildColumnType, HashedType>(
                build_input_table, column_ids.first, histograms_build_column, radix_bits, build_side_bloom_filter,
                input_bloom_filter);
    };

    const auto materialize_probe_side = [&](const auto& input_bloom_filter) {
            materialized_probe_column = materialize_input<ProbeColumnType, HashedType>(
                probe_input_table, column_ids.second, histograms_probe_column, radix_bits, probe_side_bloom_filter,
                input_bloom_filter);
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
}

#endif //JOIN_HASH_STEPS_HPP
