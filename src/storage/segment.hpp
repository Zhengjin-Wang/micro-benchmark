//
// Created by lilac on 25-4-10.
//

#ifndef SEGMENT_HPP
#define SEGMENT_HPP
#include <variant>
#include <string>
#include <vector>
#include <random>
#include <memory>
#include "../types.hpp"


// 支持的基础数据类型

// 列定义（名称 + 类型）
struct ColumnDefinition {
    // std::string name;
    DataType type;
    bool is_pk;
    float range_lower_bound; // only valid for int and float, invalid for pk and string
    float range_upper_bound; // only valid for int and float, invalid for pk and string
};


class IntSegmentPosition final {
public:
    IntSegmentPosition()
    : _value(0), _null_value(false), _chunk_offset() {}

    IntSegmentPosition(const int& value, const bool null_value, const ChunkOffset& chunk_offset)
        : _value{value}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

    const int& value() const {
        return _value;
    }

    bool is_null() const {
        return _null_value;
    }

    ChunkOffset chunk_offset() const {
        return _chunk_offset;
    }

private:
    // The alignment improves the suitability of the iterator for (auto-)vectorization
    alignas(8) int _value;
    alignas(8) bool _null_value;
    alignas(8) ChunkOffset _chunk_offset;
};


class SegmentPosition final {
public:
    SegmentPosition()
    : _value(DataType()), _null_value(false), _chunk_offset() {}

    SegmentPosition(const DataType& value, const bool null_value, const ChunkOffset& chunk_offset)
        : _value{value}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

    const DataType& value() const {
        return _value;
    }

    bool is_null() const {
        return _null_value;
    }

    ChunkOffset chunk_offset() const {
        return _chunk_offset;
    }

private:
    // The alignment improves the suitability of the iterator for (auto-)vectorization
    alignas(8) DataType _value;
    alignas(8) bool _null_value;
    alignas(8) ChunkOffset _chunk_offset;
};

// 段（Segment）基类 - 代表单列数据存储
class BaseSegment {
public:
    explicit BaseSegment(bool is_pk, float range_lower_bound, float range_upper_bound, int32_t num_rows = 0):
    _is_pk(is_pk), _range_lower_bound(range_lower_bound), _range_upper_bound(range_upper_bound){};
    virtual ~BaseSegment() = default;
    virtual void generate_random(size_t num_rows) = 0; // 随机生成数据
    bool is_pk() const { return _is_pk; }

    static std::shared_ptr<BaseSegment> create_segment(const DataType& type, bool is_pk, float range_lower_bound, float range_upper_bound, int32_t num_rows = 0);

    // const SegmentPosition at(int32_t i) const {
    //     return {_data[i], _null_values[i], i};
    // }
    //
    // const std::vector<DataType>& data() const {
    //     return _data;
    // }
    virtual ChunkOffset size() const = 0;

protected:
    bool _is_pk;
    float _range_lower_bound;
    float _range_upper_bound;
    // std::vector<DataType> _data;
    std::vector<bool> _null_values;
};

// 具体段类型
// 整数段
class IntSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    IntSegment(bool is_pk, float range_lower_bound, float range_upper_bound, int32_t num_rows = 0): BaseSegment(is_pk, range_lower_bound, range_upper_bound, num_rows) {
        resize(num_rows);
    };

    void generate_pk(size_t num_rows, size_t start_offset = 1) {
        _data.clear();
        resize(num_rows);
        for (int i = (int) start_offset; i <= (int) (start_offset + num_rows - 1); ++i) {
            _data[i] = i;
        }
    }

    void generate_random(size_t num_rows) override {
        std::random_device rd;
        std::mt19937 gen(1);
        std::uniform_int_distribution<> dis(static_cast<int>(_range_lower_bound), static_cast<int>(_range_upper_bound));
        // std::cout << static_cast<int>(_range_lower_bound) << " " << static_cast<int>(_range_upper_bound) << std::endl;
        _data.clear();
        resize(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            int val = dis(gen);
            // if (val < 1) std::cout << "less than 1" << std::endl;
            _data[i] = val;
        }
    }

    const IntSegmentPosition at(int32_t i) const {
        return {_data[i], _null_values[i], i};
    }

    void resize(int32_t num_rows) {
        _data.resize(num_rows);
        _null_values.resize(num_rows);
    }

    ChunkOffset size() const override {
        return _data.size();
    }

    std::vector<int> _data;

};

// 浮点数段
class FloatSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    FloatSegment(bool is_pk, float range_lower_bound, float range_upper_bound, int32_t num_rows = 0): BaseSegment(is_pk, range_lower_bound, range_upper_bound, num_rows) {
        resize(num_rows);
    };

    void generate_random(size_t num_rows) override {
        std::random_device rd;
        std::mt19937 gen(rd());    // 伪随机数生成器
        std::uniform_real_distribution<float> dis(_range_lower_bound, _range_upper_bound);

        _data.clear();
        resize(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            float val = dis(gen);
            _data[i] = val;
        }
    }

    float at(size_t i) const {
        return _data[i];
    }

    void resize(int32_t num_rows) {
        _data.resize(num_rows);
        _null_values.resize(num_rows);
    }

    ChunkOffset size() const override {
        return _data.size();
    }

    std::vector<float> _data;

};


// 字符串段
class StringSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    StringSegment(bool is_pk, float range_lower_bound, float range_upper_bound, int32_t num_rows = 0): BaseSegment(is_pk, range_lower_bound, range_upper_bound, num_rows) {
        resize(num_rows);
    };

    void generate_random(size_t num_rows) override {
        const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::uniform_int_distribution<> len_dis(5, 15);    // 随机生成字符串长度
        std::uniform_int_distribution<> char_dis(0, chars.size() - 1);

        _data.clear();
        resize(num_rows);
        for (size_t i = 0; i < num_rows; ++i) {
            std::string str;
            int len = len_dis(_gen);
            for (int j = 0; j < len; ++j) {
                str += chars[char_dis(_gen)];
            }
            _data[i] = str;
        }
    }

    std::string at(size_t i) const {
        return _data[i];
    }

    void resize(int32_t num_rows) {
        _data.resize(num_rows);
        _null_values.resize(num_rows);
    }

    ChunkOffset size() const override {
        return _data.size();
    }

    std::vector<std::string> _data;

private:
    std::mt19937 _gen{std::random_device{}()};
};


// Chunk：生成segment
std::shared_ptr<BaseSegment> BaseSegment::create_segment(const DataType& type, bool is_pk, float range_lower_bound, float range_upper_bound, int32_t num_rows) {
    if (std::holds_alternative<int>(type)) {
        return std::make_shared<IntSegment>(is_pk, range_lower_bound, range_upper_bound, num_rows);
    }
    else if (std::holds_alternative<float>(type)) {
        return std::make_shared<FloatSegment>(is_pk, range_lower_bound, range_upper_bound, num_rows);
    }
    else if (std::holds_alternative<std::string>(type)) {
        return std::make_shared<StringSegment>(is_pk, range_lower_bound, range_upper_bound, num_rows);
    }

    throw std::runtime_error("Unsupported data type");
}
#endif //SEGMENT_HPP
