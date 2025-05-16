//
// Created by MECHREVO on 25-5-16.
//

#ifndef AGGREGATE_SEGMENT_HPP
#define AGGREGATE_SEGMENT_HPP
#include <vector>
#include <unordered_map>
#include <memory>
#include "../types.hpp"
#include <unordered_set>


// 基础数据结构定义
template<typename T>
struct AggregateResult {
    T accumulator;  // 累加器
    size_t aggregate_count;  // 计数
    RowID row_id;  // 行ID

    AggregateResult() : aggregate_count(0) {}
};

template<typename T>
struct AggregateContext {
    std::unordered_map<size_t, size_t> result_ids;  // 结果ID映射
    std::vector<AggregateResult<T>> results;  // 结果数组
};

// 简化的聚合函数实现
template<typename ColumnDataType, typename AggregateType>
class AggregateFunction {
public:
    virtual void operator()(const ColumnDataType& value, size_t& count, AggregateType& accumulator) = 0;
    virtual ~AggregateFunction() = default;
};

// 具体聚合函数实现示例
template<typename ColumnDataType, typename AggregateType>
class SumAggregator : public AggregateFunction<ColumnDataType, AggregateType> {
public:
    void operator()(const ColumnDataType& value, size_t& count, AggregateType& accumulator) override {
        accumulator += value;
    }
};

template<typename ColumnDataType, typename AggregateType>
class CountDistinctAggregator : public AggregateFunction<ColumnDataType, std::unordered_set<AggregateType>> {
public:
    void operator()(const ColumnDataType& value, size_t& count,
                   std::unordered_set<ColumnDataType>& accumulator) override {
        accumulator.insert(value);
    }
};

// 辅助函数：获取或添加结果
template<typename AggregateType>
AggregateResult<AggregateType>& get_or_add_result(
    std::unordered_map<size_t, size_t>& result_ids,
    std::vector<AggregateResult<AggregateType>>& results,
    size_t key,
    const RowID& row_id
) {
    auto it = result_ids.find(key);
    if (it != result_ids.end()) {
        return results[it->second];
    }

    // 新键，添加结果
    const auto result_id = results.size();
    result_ids[key] = result_id;
    results.emplace_back();
    results[result_id].row_id = row_id;
    return results[result_id];
}

// 重构后的聚合函数
template<typename ColumnDataType, typename AggregateType>
void aggregate_segment(
    ChunkID chunk_id,
    const std::vector<ColumnDataType>& segment_data,
    const std::vector<bool>& null_flags,
    const std::vector<size_t>& keys,
    std::shared_ptr<AggregateContext<AggregateType>> context,
    std::shared_ptr<AggregateFunction<ColumnDataType, AggregateType>> aggregator
) {
    auto& result_ids = context->result_ids;
    auto& results = context->results;

    for (size_t offset = 0; offset < segment_data.size(); ++offset) {
        // 获取或创建结果
        auto& result = get_or_add_result(
            result_ids,
            results,
            keys[offset],
            RowID{chunk_id, static_cast<ChunkOffset>(offset)}
        );

        // 处理非空值
        if (!null_flags[offset]) {
            if constexpr (std::is_same_v<AggregateType, std::unordered_set<ColumnDataType>>) {
                // CountDistinct 特殊处理
                result.accumulator.insert(segment_data[offset]);
            } else {
                // 普通聚合
                (*aggregator)(segment_data[offset], result.aggregate_count, result.accumulator);
            }
            ++result.aggregate_count;
        }
    }
}

// // 使用示例
// void example_usage() {
//     // 定义数据类型
//     using ColumnType = int;
//     using AggregateType = int;
//
//     // 准备数据
//     std::vector<ColumnType> data = {1, 2, 3, 4, 5};
//     std::vector<bool> null_flags = {false, false, false, false, false};
//     std::vector<size_t> keys = {1, 1, 2, 2, 2};  // 分组键
//
//     // 创建上下文
//     auto context = std::make_shared<AggregateContext<AggregateType>>();
//
//     // 创建聚合器
//     auto aggregator = std::make_shared<SumAggregator<ColumnType, AggregateType>>();
//
//     // 执行聚合
//     aggregate_segment<ColumnType, AggregateType>(
//         ChunkID{0},
//         data,
//         null_flags,
//         keys,
//         context,
//         aggregator
//     );
// }

#endif //AGGREGATE_SEGMENT_HPP
