#pragma once

#include <variant>
#include <string>
#include <vector>
#include <random>
#include <memory>


// 支持的基础数据类型
using DataType = std::variant<int, float, std::string>;

// 列定义（名称 + 类型）
struct ColumnDefinition {
    // std::string name;
    DataType type;
    bool is_pk;
    float range_lower_bound; // only valid for int and float, invalid for pk and string
    float range_upper_bound; // only valid for int and float, invalid for pk and string
};

// 段（Segment）基类 - 代表单列数据存储
class BaseSegment {
public:
    explicit BaseSegment(bool is_pk, float range_lower_bound, float range_upper_bound):
    _is_pk(is_pk), _range_lower_bound(range_lower_bound), _range_upper_bound(range_upper_bound){};
    virtual ~BaseSegment() = default;
    virtual void generate_random(size_t num_rows) = 0; // 随机生成数据
    virtual const std::vector<DataType>& data() const = 0;
    bool is_pk() const { return _is_pk; }

    static std::shared_ptr<BaseSegment> create_segment(const DataType& type, bool is_pk, float range_lower_bound, float range_upper_bound);

protected:
    bool _is_pk;
    float _range_lower_bound;
    float _range_upper_bound;
};

// 具体段类型
// 整数段
class IntSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    void generate_pk(size_t num_rows, size_t start_offset = 1) {
        for (int i = (int) start_offset; i <= (int) (start_offset + num_rows - 1); ++i) {
            _data.emplace_back(i);
        }
    }

    void generate_random(size_t num_rows) override {
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(static_cast<int>(_range_lower_bound), static_cast<int>(_range_upper_bound));
        
        _data.clear();
        for (size_t i = 0; i < num_rows; ++i) {
            _data.emplace_back(dis(gen));
        }
    }

    int at(size_t i) const {
        return std::get<int>(data()[i]);
    }

    const std::vector<DataType>& data() const override {
        return _data;
    }

private:
    std::vector<DataType> _data;
};

// 浮点数段
class FloatSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    void generate_random(size_t num_rows) override {
        std::random_device rd;
        std::mt19937 gen(rd());    // 伪随机数生成器
        std::uniform_real_distribution<float> dis(_range_lower_bound, _range_upper_bound);
        
        _data.clear();
        for (size_t i = 0; i < num_rows; ++i) {
            _data.emplace_back(dis(gen)); // 存储为 float 类型
        }
    }

    float at(size_t i) const {
        return std::get<float>(data()[i]);
    }

    const std::vector<DataType>& data() const override { 
        return _data; 
    }

private:
    std::vector<DataType> _data; // 必须定义数据存储
};


// 字符串段
class StringSegment : public BaseSegment {
public:
    using BaseSegment::BaseSegment;

    void generate_random(size_t num_rows) override {
        const std::string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        std::uniform_int_distribution<> len_dis(5, 15);    // 随机生成字符串长度
        std::uniform_int_distribution<> char_dis(0, chars.size() - 1);
        
        _data.clear();
        for (size_t i = 0; i < num_rows; ++i) {
            std::string str;
            int len = len_dis(_gen);
            for (int j = 0; j < len; ++j) {
                str += chars[char_dis(_gen)];
            }
            _data.emplace_back(str); // 存储为 string 类型
        }
    }

    std::string at(size_t i) const {
        return std::get<std::string>(data()[i]);
    }

    const std::vector<DataType>& data() const override { 
        return _data; 
    }

private:
    std::vector<DataType> _data; // 必须定义数据存储
    std::mt19937 _gen{std::random_device{}()};
};


// Chunk：生成segment
std::shared_ptr<BaseSegment> BaseSegment::create_segment(const DataType& type, bool is_pk, float range_lower_bound, float range_upper_bound) {
    if (std::holds_alternative<int>(type)) {
        return std::make_shared<IntSegment>(is_pk, range_lower_bound, range_upper_bound);
    }
    else if (std::holds_alternative<float>(type)) {
        return std::make_shared<FloatSegment>(is_pk, range_lower_bound, range_upper_bound);
    }
    else if (std::holds_alternative<std::string>(type)) {
        return std::make_shared<StringSegment>(is_pk, range_lower_bound, range_upper_bound);
    }

    throw std::runtime_error("Unsupported data type");
}