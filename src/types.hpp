//
// Created by zhengjin.wang on 25-4-29.
//

#ifndef TYPES_HPP
#define TYPES_HPP
#include <iostream>
#include <tuple>
#include <variant>

using DataType = std::variant<int, float, std::string>;
using ColumnID = uint16_t;
using ColumnCount = uint16_t;
using ChunkOffset = uint32_t;
using ChunkID = uint32_t;

using CommitID = uint32_t;
using TransactionID = uint32_t;

constexpr auto INVALID_TRANSACTION_ID = TransactionID{0};
constexpr auto INITIAL_TRANSACTION_ID = TransactionID{1};

constexpr ChunkOffset INVALID_CHUNK_OFFSET{UINT32_MAX};
constexpr ChunkID INVALID_CHUNK_ID{UINT32_MAX};

struct RowID {
    constexpr RowID(const ChunkID init_chunk_id, const ChunkOffset init_chunk_offset)
        : chunk_id{init_chunk_id}, chunk_offset{init_chunk_offset} {}

    RowID() = default;

    ChunkID chunk_id{INVALID_CHUNK_ID};
    ChunkOffset chunk_offset{INVALID_CHUNK_OFFSET};

    // Faster than row_id == NULL_ROW_ID, since we only compare the ChunkOffset.
    bool is_null() const {
        return chunk_offset == INVALID_CHUNK_OFFSET;
    }

    // Joins need to use RowIDs as keys for maps.
    bool operator<(const RowID& other) const {
        return std::tie(chunk_id, chunk_offset) < std::tie(other.chunk_id, other.chunk_offset);
    }

    // Useful when comparing a row ID to NULL_ROW_ID
    bool operator==(const RowID& other) const {
        return std::tie(chunk_id, chunk_offset) == std::tie(other.chunk_id, other.chunk_offset);
    }

    friend std::ostream& operator<<(std::ostream& stream, const RowID& row_id) {
        stream << "RowID(" << row_id.chunk_id << "," << row_id.chunk_offset << ")";
        return stream;
    }
};

#endif //TYPES_HPP
