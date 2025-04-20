#include "database.h"
#define ITEMSNUM 100000
void increment_key(char *key);
int main()
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    table->add_column("key", rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::INT, 4));
    char row[] = "00000000000000000000000000000000abcdefghij01-01-2002aaaa";
    for (int i = 0; i < ITEMSNUM; i++)
    {
        // std::cout << i << std::endl;
        table->insert_row_bin(row);
        increment_key(row);
    }
    delete table;
    delete db;
    return 0;
}
void increment_key(char *key)
{
    size_t cur_idx = PKEY_COL_W;
    bool move_left = true;
    while (move_left && cur_idx > 0)
    {
        cur_idx--;
        key[cur_idx]++;
        if (key[cur_idx] != 0)
        {
            move_left = false;
        }
    }
}