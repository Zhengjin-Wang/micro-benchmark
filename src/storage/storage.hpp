//
// Created by lilac on 25-4-10.
//

#ifndef STORAGE_HPP
#define STORAGE_HPP
#include <vector>
#include <memory>
#include <variant>

#include "mvcc_data.h"
#include "segment.hpp"
#include "../types.hpp"
#include "../assert.h"

#define CHUNK_SIZE 65536

// 块（Chunk）- 包含多个段（列）
class Chunk {
public:
    // column_defs：存储对应列的数据类型
    Chunk(const std::vector<ColumnDefinition>& column_defs, int32_t num_rows = 0) {
        for (const auto& def : column_defs) {
            _segments.emplace_back(BaseSegment::create_segment(def.type, def.is_pk, def.range_lower_bound, def.range_upper_bound, num_rows));
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

    std::shared_ptr<BaseSegment> get_segment(ColumnID column_id) const {
        return _segments[column_id];
    }

    const std::vector<std::shared_ptr<BaseSegment>>& segments() const {
        return _segments;
    }

    size_t row_count() const {
        return _row_count;
    }

    ChunkOffset size() const {
        if (_segments.empty()) {
            return ChunkOffset{0};
        }
        const auto first_segment = this->get_segment(ColumnID{0});
        return static_cast<ChunkOffset>(first_segment->size());
    }

    bool is_mutable() const {
        return _is_mutable.load();
    }

    void set_immutable() {
        auto success = true;
        Assert(_is_mutable.compare_exchange_strong(success, false), "Only mutable chunks can be set immutable.");

        // Only perform the `max_begin_cid` check if it has not already been set.
        if (has_mvcc_data() && _mvcc_data->max_begin_cid.load() == MvccData::MAX_COMMIT_ID) {
            const auto chunk_size = size();
            Assert(chunk_size > 0, "`set_immutable()` should not be called on an empty chunk.");
            auto max_begin_cid = CommitID{0};
            for (auto chunk_offset = ChunkOffset{0}; chunk_offset < chunk_size; ++chunk_offset) {
                max_begin_cid = std::max(max_begin_cid, _mvcc_data->get_begin_cid(chunk_offset));
            }
            set_atomic_max(_mvcc_data->max_begin_cid, max_begin_cid);

            Assert(_mvcc_data->max_begin_cid != MvccData::MAX_COMMIT_ID,
                   "`max_begin_cid` should not be MAX_COMMIT_ID when marking a chunk as immutable.");
        }
    }

private:
    size_t _row_count;
    std::vector<std::shared_ptr<BaseSegment>> _segments;
    std::atomic_bool _is_mutable{true};
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

    void append_mutable_chunk() {
        auto chunk = std::make_shared<Chunk>(_column_defs, CHUNK_SIZE);

        // auto mvcc_data = std::shared_ptr<MvccData>{};
        // if (_use_mvcc == UseMvcc::Yes) {
        //     mvcc_data = std::make_shared<MvccData>(_target_chunk_size, MvccData::MAX_COMMIT_ID);
        // }

        // append_chunk(segments, mvcc_data);
        _chunks.push_back(chunk);
    }

    void output_data(const std::string& filepath, const std::vector<ColumnID>& column_ids) {
        std::ofstream out_file(filepath);
        for (auto& chunk : _chunks) {
            size_t n = chunk->row_count();
            for (size_t i = 0; i < n; i++) {
                std::string s = "";
                for (auto& column_id: column_ids) {
                    auto segment = chunk->get_segment(column_id);
                    auto& type = _column_defs[column_id].type;
                    if (std::holds_alternative<int>(type)) {
                        s += std::to_string(std::dynamic_pointer_cast<IntSegment>(segment)->_data[i]);
                    }
                    else if (std::holds_alternative<float>(type)) {
                        s += std::to_string(std::dynamic_pointer_cast<FloatSegment>(segment)->_data[i]);
                    }
                    else if (std::holds_alternative<std::string>(type)) {
                        s += std::dynamic_pointer_cast<StringSegment>(segment)->_data[i];
                    }
                    if (column_id != column_ids.back()) {
                        s += ",";
                    }
                }
                s += "\n";
                out_file << s;
            }
        }
        out_file.close();
        std::cout << "Successfully saved table to " << filepath << std::endl;
    }

    std::unique_lock<std::mutex> acquire_append_mutex() {
        return std::unique_lock<std::mutex>(*_append_mutex);
    }

private:
    std::vector<ColumnDefinition> _column_defs;    // column_defs：不同列的数据类型
    std::vector<std::shared_ptr<Chunk>> _chunks;
    size_t _chunk_size;    // 每个chunk的行数
    size_t _row_count;
    std::unique_ptr<std::mutex> _append_mutex;
};
#endif //STORAGE_HPP
