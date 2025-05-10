//
// Created by lilac on 25-4-10.
//

#ifndef STORAGE_HPP
#define STORAGE_HPP
#include <vector>
#include <memory>
#include <variant>

#include "mvcc_data.hpp"
#include "segment.hpp"
#include "../types.hpp"
#include "../assert.hpp"
#include "../utils/atomic_max.hpp"

#define CHUNK_SIZE 65536

// 块（Chunk）- 包含多个段（列）
class Chunk {
public:
    // column_defs：存储对应列的数据类型
    Chunk(const std::vector<ColumnDefinition>& column_defs, int32_t num_rows, const std::shared_ptr<MvccData>& mvcc_data) : _column_defs(column_defs), _mvcc_data(mvcc_data){
        for (const auto& def : column_defs) {
            _segments.emplace_back(BaseSegment::create_segment(def.type, def.is_pk, def.range_lower_bound, def.range_upper_bound, num_rows));
        }
    }

    // referenced segment
    Chunk(const std::vector<ColumnDefinition>& column_defs, const std::vector<std::shared_ptr<BaseSegment>>& segments) : _column_defs(column_defs), _segments(segments){
    }

    // 生成该块的所有列数据
    void generate_data(size_t num_rows, size_t start_offset = 1) {
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

    bool has_mvcc_data() const {

        return _mvcc_data != nullptr;
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

    std::shared_ptr<MvccData> mvcc_data() const {
        return _mvcc_data;
    }

    ColumnCount column_count() const {
        return _segments.size();
    }

    const std::vector<ColumnDefinition>& column_defs() const {
        return _column_defs;
    }

    void mark_as_full() {
        _reached_target_size = true;
    }

private:
    std::vector<ColumnDefinition> _column_defs;    // column_defs：不同列的数据类型
    std::shared_ptr<MvccData> _mvcc_data;
    std::vector<std::shared_ptr<BaseSegment>> _segments;
    std::atomic_bool _is_mutable{true};
    std::atomic_bool _reached_target_size{false};
};

// 表（Table）- 包含多个块
class Table {
public:
    Table(std::vector<ColumnDefinition> column_defs)    // 默认行数为65536
        : _column_defs(std::move(column_defs)),
          _append_mutex(std::make_unique<std::mutex>()){}

    // 生成指定行数的数据
    void generate_data(size_t total_rows) {
        size_t remaining = total_rows;
        size_t start_index = 1;
        while (remaining > 0) {
            size_t rows_in_chunk = std::min(remaining, (size_t) CHUNK_SIZE);
            auto mvcc_data = std::make_shared<MvccData>(CHUNK_SIZE, CommitID{0});
            auto chunk = std::make_shared<Chunk>(_column_defs, rows_in_chunk, mvcc_data);
            chunk->generate_data(rows_in_chunk, start_index);
            _chunks.push_back(chunk);
            remaining -= rows_in_chunk;
            start_index += rows_in_chunk;
        }
    }

    size_t row_count() const {
        auto row_count = uint64_t{0};
        const auto chunk_count = _chunks.size();
        for (auto chunk_id = ChunkID{0}; chunk_id < chunk_count; ++chunk_id) {
            const auto chunk = get_chunk(chunk_id);
            if (chunk) {
                row_count += chunk->size();
            }
        }
        return row_count;
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
        auto mvcc_data = std::make_shared<MvccData>(CHUNK_SIZE, CommitID{0});
        auto chunk = std::make_shared<Chunk>(_column_defs, 0, mvcc_data);

        // auto mvcc_data = std::shared_ptr<MvccData>{};
        // if (_use_mvcc == UseMvcc::Yes) {
        //     mvcc_data = std::make_shared<MvccData>(_target_chunk_size, MvccData::MAX_COMMIT_ID);
        // }

        // append_chunk(segments, mvcc_data);
        _chunks.push_back(chunk);
    }

    void append_reference_chunk(const std::shared_ptr<Chunk>& chunk) {
        _chunks.push_back(chunk);
    }

    ChunkOffset target_chunk_size() const {
        return CHUNK_SIZE;
    }

    void output_data(const std::string& filepath, const std::vector<ColumnID>& column_ids) {
        std::ofstream out_file(filepath);
        for (auto& chunk : _chunks) {
            size_t n = chunk->size();
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

    const DataType& column_data_type(ColumnID column_id) const {
        return _column_defs[column_id].type;
    }

    const std::vector<ColumnDefinition>& column_defs() const {
        return _column_defs;
    }

private:
    std::vector<ColumnDefinition> _column_defs;    // column_defs：不同列的数据类型
    std::vector<std::shared_ptr<Chunk>> _chunks;
    std::unique_ptr<std::mutex> _append_mutex;
};
#endif //STORAGE_HPP
