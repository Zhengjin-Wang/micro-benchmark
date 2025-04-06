//
// Created by lilac on 25-4-6.
//

#ifndef JOIN_HASH_STEPS_HPP
#define JOIN_HASH_STEPS_HPP
#include <cstdint>
#include <bits/stdc++.h>

using namespace std;

using ChunkOffset = int32_t;

template <typename T>
class SegmentPosition final {
 public:
  SegmentPosition()
  : _value(T()), _null_value(false), _chunk_offset() {}

  SegmentPosition(const T& value, const bool null_value, const ChunkOffset& chunk_offset)
      : _value{value}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const T& value() const {
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
  alignas(8) T _value;
  alignas(8) bool _null_value;
  alignas(8) ChunkOffset _chunk_offset;
};

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
using Segment = std::vector<SegmentPosition<T>>;

template <typename T>
using RadixContainer = std::vector<Partition<T>>;

std::vector<int> get_chunk_list(const string& filename){
    ifstream file(filename);
    vector<int> numbers;           // 存储数字的vector

    if (!file.is_open()) {
        cerr << "无法打开文件！" << endl;
        return {};
    }

    string line;
    while (getline(file, line)) {  // 逐行读取
        stringstream ss(line);     // 将行内容转为字符串流
        int num;
        while (ss >> num) {        // 按空格/逗号分隔解析数字
            numbers.push_back(num);
        }
    }

    file.close();  // 关闭文件
    return numbers;
}

static constexpr auto BLOOM_FILTER_SIZE = 1 << 20;
static constexpr auto BLOOM_FILTER_MASK = BLOOM_FILTER_SIZE - 1;
using Hash = size_t;

template <typename T>
RadixContainer<T> materialize_input(const std::vector<Segment<T>>& fake_table){

  const auto chunk_count = fake_table.size();

  // bloom_filter resize
  vector<int> fake_bloom_filter;
  fake_bloom_filter.resize(BLOOM_FILTER_SIZE >> 5);

  const std::hash<T> hash_function;

  // histograms resize
  std::vector<std::vector<size_t>> histograms;
  histograms.resize(chunk_count);
  size_t fake_radix_bits{8};
  auto radix_bits = fake_radix_bits;
  const size_t num_radix_partitions = 1ull << radix_bits;

  const auto pass = size_t{0};
  const auto radix_mask = static_cast<size_t>(std::pow(2, radix_bits * (pass + 1)) - 1);

  // radix_container copy
  auto radix_container = RadixContainer<T>{};
  radix_container.resize(chunk_count);

  // copy memory
  for (int32_t chunk_id = 0; chunk_id < chunk_count; ++chunk_id){

      auto& elements = radix_container[chunk_id].elements;
      auto& null_values = radix_container[chunk_id].null_values;
      auto& segment = fake_table[chunk_id];
      const auto num_rows = segment.size();
      // std::cerr << "Number of rows in one chunk: " << num_rows << std::endl;

      elements.resize(num_rows);
      null_values.resize(num_rows);

      auto histogram = std::vector<size_t>(num_radix_partitions);

      for (size_t i = 0; i < num_rows; ++i) {
        auto& value = segment[i];

        const auto hashed_value = hash_function(static_cast<T>(value.value()));
        auto skip = false;
        if(false) {
          skip = true;
        }

        if(!skip) {
          fake_bloom_filter[hashed_value & BLOOM_FILTER_MASK] = 1;

          elements[i] = PartitionedElement<T>{RowID{chunk_id, value.chunk_offset()}, value.value()};
          null_values[i] = segment[i].is_null();

          if (radix_bits > 0) {
            const Hash radix = hashed_value & radix_mask;
            ++histogram[radix];
          }
        }
      }

      histograms[chunk_id] = std::move(histogram);

  }

  return radix_container;
}

// 模拟单个查询/模拟负载
template <typename T>
void sim(){

    // generate data
    std::vector<int> chunk_list = get_chunk_list("data.txt");
    int chunk_count = chunk_list.size();
    int n = chunk_list.size();

    vector<vector<Segment<T>>> fake_tables; // one table may contain a few chunks
    for(int i = 0; i < n; ++i){
      vector<int> tmp;
      while(i < n && chunk_list[i] >= 65535){
         tmp.push_back(chunk_list[i]);
         ++i;
      }
      tmp.push_back(chunk_list[i]);

      std::vector<Segment<T>> fake_table{};
      for(int chunk_size:tmp){
        Segment<T> segment;
        segment.resize(chunk_size);
        for(auto& data: segment){
          SegmentPosition<T> sp2(rand(), false, rand());
           data = move(sp2);
        }
        fake_table.push_back(segment);
      }
      fake_tables.push_back(fake_table);
    }

    // count time
    const auto start = std::chrono::high_resolution_clock::now();

    for(auto& fake_table: fake_tables){
      materialize_input(fake_table);
    }

    const auto end = std::chrono::high_resolution_clock::now();
    const auto duration = std::chrono::duration<double>(end - start).count();
    std::cerr << "Time taken in copy memory: " << duration << "s" << std::endl;

}


int main() {
    sim<int>();
}
#endif //JOIN_HASH_STEPS_HPP
