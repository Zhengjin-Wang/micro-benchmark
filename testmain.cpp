#include <vector>
#include <iostream>
#include <memory>

#include "join_hash.hpp"
#include "storage.hpp"
#include "segment.hpp"

int main() {

    size_t pk_lower_bound = 1;
    size_t pk_upper_bound = 200000;

    size_t m = 159526; // r_table_size
    size_t n = 6001215; // s_table_size
    size_t CHUNK_SIZE = 65536;


    // 定义表结构：三列（int, float, string）
    std::vector<ColumnDefinition> r_column_defs = {
        {int{}, true, 0, 0},
        {int{}, false, 1, 100},
        {std::string{}, false, 0, 0}
    };
    const auto& r_table = std::make_shared<Table>(r_column_defs, CHUNK_SIZE);

    std::vector<ColumnDefinition> s_column_defs = {
        {int{}, false, (float) pk_lower_bound, (float) pk_upper_bound},
        {int{}, false, 1, 50},
        {float{}, false, 900.0, 9000.0},
        {float{}, false, 0.01, 0.10},
        {float{}, false, 0.01, 0.10},
        {int{}, false, 1, 7}
    };
    const auto& s_table = std::make_shared<Table>(s_column_defs, CHUNK_SIZE);

    r_table->generate_data(m); // 生成m行数据
    s_table->generate_data(n); // 生成n行数据
    std::pair<ColumnID, ColumnID> column_ids({0, 0});
    join_hash<int, int, int>(r_table, s_table, column_ids, 10);

    // 打印数据
    // const auto& r_chunk = r_table.chunks()[0];
    // const auto& r_segment =    std::dynamic_pointer_cast<IntSegment>(r_chunk->get_segment(0));
    //
    // const auto& s_chunk = s_table.chunks()[0];
    // const auto& s_segment =    std::dynamic_pointer_cast<IntSegment>(s_chunk->get_segment(0));

    // for (size_t i = 0; i < m; ++i) {
    //     int id = r_segment->at(i);
    //     std::cout << "r-id: " << id << std::endl;
    // }
    //
    // for (size_t i = 0; i < n; ++i) {
    //     int id = s_segment->at(i);
    //     std::cout << "s-id: " << id << std::endl;
    // }

    return 0;
}
// int main() {
//     // 定义表结构：两列（int, string）
//     std::vector<ColumnDefinition> column_defs = {
//         {"id", int{}},       // 类型占位符
//         {"name", std::string{}}
//     };

//     // 创建表并生成10000行数据
//     Table table(column_defs, 1000); // Chunk大小=1000
//     table.generate_random(10000);

//     // 访问数据示例
//     const auto& first_chunk = table.chunks()[0];
//     const auto& id_segment = first_chunk->segments()[0];
//     const auto& name_segment = first_chunk->segments()[1];

//     // 打印前5行
//     for (size_t i = 0; i < 5; ++i) {
//         int id = std::get<int>(id_segment->data()[i]);
//         std::string name = std::get<std::string>(name_segment->data()[i]);
//         std::cout << "ID: " << id << ", Name: " << name << std::endl;
//     }

//     return 0;
// }