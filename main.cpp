#include <chrono>
#include <vector>
#include <iostream>
#include <memory>
#include <getopt.h>

#include "src/operator/join_hash.hpp"
#include "src/operator/insert.hpp"
#include "src/operator/delete.hpp"
#include "src/operator/update.hpp"
#include "src/operator/aggregate_hash.hpp"
#include "src/storage/storage.hpp"
#include "src/storage/segment.hpp"
#include "src/storage/reference_segment.hpp"

std::vector<std::shared_ptr<Table>> generate_tables(int sf, bool output_data, std::string r_table_filepath, std::string s_table_filepath) {
    // std::cout << "sf=" << sf << std::endl;
    size_t m = 159526 * sf; // r_table_size
    size_t n = 6001215 * sf; // s_table_size
    size_t pk_lower_bound = 1;
    size_t pk_upper_bound = 200000 * sf;


    // 定义表结构：三列（int, float, string）
    std::vector<ColumnDefinition> r_column_defs = {
        {int{}, true, 0, 0},
        // {int{}, false, 1, 100},
        // {std::string{}, false, 0, 0}
    };
    const auto r_table = std::make_shared<Table>(r_column_defs);

    std::vector<ColumnDefinition> s_column_defs = {
        {int{}, false, (float) pk_lower_bound, (float) pk_upper_bound},
        // {int{}, false, 1, 50},
        // {float{}, false, 900.0, 9000.0},
        // {float{}, false, 0.01, 0.10},
        // {float{}, false, 0.01, 0.10},
        // {int{}, false, 1, 7}
    };
    const auto s_table = std::make_shared<Table>(s_column_defs);

    auto start = std::chrono::high_resolution_clock::now();
    r_table->generate_data(m); // 生成m行数据
    s_table->generate_data(n); // 生成n行数据
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    std::cerr << "Generate data time: " << duration << "s" << std::endl;
    if (output_data){
        r_table->output_data(r_table_filepath, {0});
        s_table->output_data(s_table_filepath, {0});
    }

    return {r_table, s_table};
}

void test_join_hash(std::shared_ptr<const Table> r_table, std::shared_ptr<const Table> s_table, const std::string& test_op) {
    // 测试join_hash
    std::pair<ColumnID, ColumnID> column_ids({0, 0});
    auto start = std::chrono::high_resolution_clock::now();
    join_hash<int, int, int>(r_table, s_table, column_ids, 10, test_op);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    // std::cerr << "join_hash time: " << duration << "s" << std::endl;
}

void test_insert(std::shared_ptr<Table> target_table, std::shared_ptr<const Table> source_table) {
    auto start = std::chrono::high_resolution_clock::now();
    insert(target_table, source_table);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    std::cerr << "insert time: " << duration << "s" << std::endl;
}

void test_delete(std::shared_ptr<Table> target_table) {
    auto start = std::chrono::high_resolution_clock::now();
    delete_rows(target_table);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    std::cerr << "delete time: " << duration << "s" << std::endl;
}

void test_update(std::shared_ptr<Table>& target_table, std::shared_ptr<const Table>& values_to_insert, std::shared_ptr<Table>& rows_to_delete) {
    auto start = std::chrono::high_resolution_clock::now();
    update(target_table, values_to_insert, rows_to_delete);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    std::cerr << "update time: " << duration << "s" << std::endl;
}

void test_aggregate_hash(std::shared_ptr<Table>& target_table, const std::vector<std::string>& aggregates, const std::vector<ColumnID>& _groupby_column_ids) {
    auto start = std::chrono::high_resolution_clock::now();
    aggregate_hash(target_table, aggregates, _groupby_column_ids);
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    std::cerr << "aggregate_hash time: " << duration << "s" << std::endl;
}

void printUsage() {
    std::cout << "Usage: program [OPTIONS]\n"
              << "Options:\n"
              << "  -h, --help          Show this help message\n"
              << "  -s, --sf       set sf(default: 1)\n"
              << "  -o, --output       if output\n"
              << "  -t, --test       which op to test(join_hash|insert)\n";
}

int main(int argc, char** argv) {

    int opt;
    const char *optString = "h?s:ot:"; // 短选项
    struct option longOpts[] = { // 长选项
        {"help", no_argument, nullptr, 'h'},
        {"sf", required_argument, nullptr, 's'},
        {"output", no_argument, nullptr, 'o'},
        {"test_op", required_argument, nullptr, 't'},
        {nullptr, 0, nullptr, 0} // 结束标志
    };

    int sf = 1;
    bool output_data = false;
    std::string test_op = "";

    while (true) {
        int optionIndex = 0;
        opt = getopt_long(argc, argv, optString, longOpts, &optionIndex);

        if (opt == -1) {
            break; // 没有更多选项
        }

        switch (opt) {
            case 'h':
                printUsage();
            return 0;
            case 's':
                sf = atoi(optarg);
            break;
            case 'o':
                output_data = true;
            break;
            case 't':
                test_op = std::move(std::string(optarg));
            break;
            case '?':
                printUsage();
            return 1;
            default:
                break;
        }
    }

    // 处理其他参数
    for (int i = optind; i < argc; i++) {
        std::cout << "Extra argument: " << argv[i] << "\n";
    }

    std::cout << "sf=" << sf << ", output_data=" << output_data<< ", test_op=" << test_op<<std::endl;

    // 生成表


    // 测试算子
    if (test_op == "join_hash" || test_op == "materialize_input" || test_op == "partition_by_radix") {
        auto tables = generate_tables(sf, output_data, "r_table.csv", "s_table.csv");
        auto r_table = tables[0];
        auto s_table = tables[1];
        test_join_hash(r_table, s_table, test_op);
    }
    else if (test_op == "insert") {
        auto tables = generate_tables(0, output_data, "r_table.csv", "s_table.csv");
        auto r_table = tables[0];
        auto s_table = tables[1];
        auto insert_tables = generate_tables(sf, output_data, "r_table_insert.csv", "s_table_insert.csv");
        auto r_insert = insert_tables[0];
        auto s_insert = insert_tables[1];
        test_insert(r_table, r_insert);
        test_insert(s_table, s_insert);
        // r_table->output_data("a.log", {0});
    }
    else if (test_op == "delete") {
        auto tables = generate_tables(sf, output_data, "r_table.csv", "s_table.csv");
        auto r_table = tables[0];
        auto s_table = tables[1];
        auto reference_r_table = ReferenceSegment::create_reference_table(r_table);
        auto reference_s_table = ReferenceSegment::create_reference_table(s_table);
        test_delete(reference_r_table);
        test_delete(reference_s_table);
    }
    else if (test_op == "update") {
        auto tables = generate_tables(sf, output_data, "r_table.csv", "s_table.csv");
        auto r_table = tables[0];
        auto s_table = tables[1];
        auto reference_r_table = ReferenceSegment::create_reference_table(r_table);
        auto reference_s_table = ReferenceSegment::create_reference_table(s_table);
        auto insert_tables = generate_tables(sf, output_data, "r_table_insert.csv", "s_table_insert.csv");
        std::shared_ptr<const Table> r_insert = insert_tables[0];
        std::shared_ptr<const Table> s_insert = insert_tables[1];
        test_update(r_table, r_insert, reference_r_table);
        test_update(s_table, s_insert, reference_s_table);
    }
    else if (test_op == "aggregate_hash") {
        auto tables = generate_tables(sf, output_data, "r_table.csv", "s_table.csv");
        auto r_table = tables[0];
        auto s_table = tables[1];
        test_aggregate_hash(s_table, {}, {0});
    }

    return 0;
}