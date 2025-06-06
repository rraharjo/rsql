#include <iostream>
#include <chrono>
#include "database.h"

#define ITEMSNUM 1000000
void increment_key(char *key);
int main()
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    table->add_column("key", rsql::Column::get_column(0, rsql::DataType::DEFAULT_KEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::UINT, 4));
    table->add_column("col_4", rsql::Column::get_column(0, rsql::DataType::UINT, 4));
    char row[] = "00000000000000000000000000000000abcdefghij2002-01-01aaaaaaaa";
    memset(row + 52, 0, 8);
    std::cout << "Inserting " << ITEMSNUM << " items..." << std::endl;
    auto before_insert = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < ITEMSNUM; i++)
    {
        uint32_t a = *reinterpret_cast<uint32_t *>(row + 52);
        table->insert_row_bin(row);
        increment_key(row);
        a++;
        std::memcpy(row + 52, &a, 4);
        std::memcpy(row + 56, &a, 4);
    }
    auto after_insert = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::milliseconds>(after_insert - before_insert);
    std::cout << ITEMSNUM << " items inserted!" << std::endl;
    std::cout << "Time elapsed: " << insert_duration.count() << " ms" << std::endl;
    std::cout << "Started indexing column..." << std::endl;
    auto before_index = std::chrono::high_resolution_clock::now();
    table->index_column("col_3");
    auto after_index = std::chrono::high_resolution_clock::now();
    auto index_duration = std::chrono::duration_cast<std::chrono::milliseconds>(after_index - before_index);
    std::cout << "Done indexing!" << std::endl;
    std::cout << "Time elapsed: " << index_duration.count() << " ms" << std::endl;
    delete table;
    delete db;
    return 0;
}
void increment_key(char *key)
{
    size_t cur_idx = DEFAULT_KEY_WIDTH;
    bool move_left = true;
    while (move_left && cur_idx > 0)
    {
        cur_idx--;
        if (key[cur_idx] == '9')
            key[cur_idx] = '0';
        else
            key[cur_idx]++;
        if (key[cur_idx] != '0')
            move_left = false;
    }
}