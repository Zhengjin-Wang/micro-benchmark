//
// Created by zhengjin.wang on 25-4-29.
//

#ifndef TYPES_HPP
#define TYPES_HPP
#include <variant>

using DataType = std::variant<int, float, std::string>;
using ChunkOffset = int32_t;
using ColumnID = uint16_t;
using ColumnCount = uint16_t;
using ChunkID = uint32_t;

using CommitID = uint32_t;
using TransactionID = uint32_t;

constexpr auto INVALID_TRANSACTION_ID = TransactionID{0};
constexpr auto INITIAL_TRANSACTION_ID = TransactionID{1};
#endif //TYPES_HPP
