//
// Created by lilac on 25-4-6.
//

#ifndef STORAGE_HPP
#define STORAGE_HPP
#pragma once

#include <vector>
#include <memory>
#include <variant>
#include <random>

#include "segment.hpp"

using ColumnID = uint16_t;

// 块（Chunk）- 包含多个段（列）
class Chunk {
public:
    // column_defs：存储对应列的数据类型
    Chunk(const std::vector<ColumnDefinition>& column_defs) {
        for (const auto& def : column_defs) {
            _segments.emplace_back(BaseSegment::create_segment(def.type, def.is_pk, def.range_lower_bound, def.range_upper_bound));
        }
    }

    // 生成该块的所有列数据
    void generate_data(size_t num_rows, size_t start_offset = 1) {
        _row_count = num_rows;
        for (auto& seg : _segments) {
            if (seg->is_pk()) std::dynamic_pointer_cast<IntSegment>(seg)->generate_pk(num_rows, start_offset);
            else seg->generate_random(num_rows);
        }
    }

    std::shared_ptr<BaseSegment> get_segment(ColumnID column_id) {
        return _segments[column_id];
    }

    const std::vector<std::shared_ptr<BaseSegment>>& segments() const {
        return _segments;
    }

    size_t row_count() const {
        return _row_count;
    }

private:
    size_t _row_count;
    std::vector<std::shared_ptr<BaseSegment>> _segments;
};

// 表（Table）- 包含多个块
class Table {
public:
    Table(std::vector<ColumnDefinition> column_defs, size_t chunk_size = 65536)    // 默认行数为65536
        : _column_defs(std::move(column_defs)), _chunk_size(chunk_size) {}

    // 生成指定行数的数据
    void generate_data(size_t total_rows) {
        _row_count = total_rows;
        size_t remaining = total_rows;
        size_t start_index = 1;
        while (remaining > 0) {
            size_t rows_in_chunk = std::min(remaining, _chunk_size);
            auto chunk = std::make_shared<Chunk>(_column_defs);
            chunk->generate_data(rows_in_chunk, start_index);
            _chunks.push_back(chunk);
            remaining -= rows_in_chunk;
            start_index += rows_in_chunk;
        }
    }

    size_t row_count() const {
        return _row_count;
    }

    size_t chunk_count() const {
        return _chunks.size();
    }

    std::shared_ptr<Chunk> get_chunk(size_t i) const {
        return _chunks[i];
    }

    const std::vector<std::shared_ptr<Chunk>>& chunks() const {
        return _chunks;
    }

private:
    std::vector<ColumnDefinition> _column_defs;    // column_defs：不同列的数据类型
    std::vector<std::shared_ptr<Chunk>> _chunks;
    size_t _chunk_size;    // 每个chunk的行数
    size_t _row_count;
};
#endif //STORAGE_HPP
