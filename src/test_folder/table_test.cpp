#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "database.h"

std::string clear_cache = "make cleancachefolder";

BOOST_AUTO_TEST_CASE(table_create_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(table_column_test){
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);
    table->add_column("key", rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    BOOST_CHECK(table->get_width() == 32);
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    BOOST_CHECK(table->get_width() == 32 + 10 + 10);
    table->remove_column("col_1");
    BOOST_CHECK(table->get_width() == 32 + 10);
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(insert_row_test){
    
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("key", rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));

    char row[] = "00000000000000000000000000000000abcdefghij01-01-2002";
    for (int i = 0 ; i < 10 ; i++){
        table->insert_row_bin(row);
        row[PKEY_COL_W - 1]++;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_row_test){
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));

    char row[] = "00000000000000000000000000000000abcdefghij01-01-2002";
    for (int i = 0 ; i < 10 ; i++){
        table->insert_row_bin(row);
        row[PKEY_COL_W - 1]++;
    }
    char key_0[] = "00000000000000000000000000000000";
    char key_5[] = "00000000000000000000000000000005";
    char key_8[] = "00000000000000000000000000000008";
    char key_idk[] = "00000000000000000000000000000010";
    std::vector<char *> found_0 = table->find_row(key_0, first_column);
    std::vector<char *> found_5 = table->find_row(key_5, first_column);
    std::vector<char *> found_8 = table->find_row(key_8, first_column);
    std::vector<char *> found_idk = table->find_row(key_idk, first_column);

    BOOST_CHECK(found_0.size() == 1);
    BOOST_CHECK(found_5.size() == 1);
    BOOST_CHECK(found_8.size() == 1);
    BOOST_CHECK(found_idk.size() == 0);
    row[PKEY_COL_W - 1] = '0';
    BOOST_CHECK(strncmp(found_0[0], row, 52) == 0);
    row[PKEY_COL_W - 1] = '5';
    BOOST_CHECK(strncmp(found_5[0], row, 52) == 0);
    row[PKEY_COL_W - 1] = '8';
    BOOST_CHECK(strncmp(found_8[0], row, 52) == 0);

    delete[] found_0[0];
    delete[] found_5[0];
    delete[] found_8[0];
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}