#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "database.h"

std::string clear_cache = "make cleancache";

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

BOOST_AUTO_TEST_CASE(table_create_test_with_columns_test){
    std::vector<std::string> col_names = {"col_0", "col_1", "col_2"};
    std::vector<rsql::Column> columns;
    columns.push_back(rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    columns.push_back(rsql::Column::get_column(0, rsql::DataType::CHAR, 100));
    columns.push_back(rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table", col_names, columns);
    BOOST_CHECK(table != nullptr);
    BOOST_CHECK(table->get_width() == 142);
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(table_column_test)
{
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

BOOST_AUTO_TEST_CASE(insert_binary_row_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("key", rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));

    char row[] = "00000000000000000000000000000000abcdefghij2002-01-01";
    for (int i = 0; i < 10; i++)
    {
        table->insert_row_bin(row);
        row[PKEY_COL_W - 1]++;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(insert_row_text_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("key", rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::INT, 32));

    std::vector<std::string> row_1 = {"00000000000000000000000000000000", "abcdefghij", "2002-01-01", "1234567890"};
    std::vector<std::string> row_2 = {"00000000000000000000000000000005", "klmnopqrst", "2002-01-01", "1000000000"};

    table->insert_row_text(row_1);
    table->insert_row_text(row_2);

    std::vector<char *> vec_row_1 = table->find_row_bin("00000000000000000000000000000000", "key");
    std::vector<char *> vec_row_2 = table->find_row_bin("00000000000000000000000000000005", "key");

    long long int row_1_int = *reinterpret_cast<long long int *>(vec_row_1[0] + 52);
    long long int row_2_int = *reinterpret_cast<long long int *>(vec_row_2[0] + 52);

    BOOST_CHECK(vec_row_1.size() == 1);
    BOOST_CHECK(strncmp(vec_row_1[0], row_1[0].c_str(), 32) == 0);
    BOOST_CHECK(strncmp(vec_row_1[0] + 32, row_1[1].c_str(), 10) == 0);
    BOOST_CHECK(strncmp(vec_row_1[0] + 42, row_1[2].c_str(), 10) == 0);
    BOOST_CHECK(row_1_int == std::stoll(row_1[3]));
    BOOST_CHECK(vec_row_2.size() == 1);
    BOOST_CHECK(strncmp(vec_row_2[0], row_2[0].c_str(), 32) == 0);
    BOOST_CHECK(strncmp(vec_row_2[0] + 32, row_2[1].c_str(), 10) == 0);
    BOOST_CHECK(strncmp(vec_row_2[0] + 42, row_2[2].c_str(), 10) == 0);
    BOOST_CHECK(row_2_int == std::stoll(row_2[3]));

    delete[] vec_row_1[0];
    delete[] vec_row_2[0];
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_binary_row_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));

    char row[] = "00000000000000000000000000000000abcdefghij2002-01-01";
    for (int i = 0; i < 10; i++)
    {
        table->insert_row_bin(row);
        row[PKEY_COL_W - 1]++;
    }
    char key_0[] = "00000000000000000000000000000000";
    char key_5[] = "00000000000000000000000000000005";
    char key_8[] = "00000000000000000000000000000008";
    char key_idk[] = "00000000000000000000000000000010";
    std::vector<char *> found_0 = table->find_row_bin(key_0, first_column);
    std::vector<char *> found_5 = table->find_row_bin(key_5, first_column);
    std::vector<char *> found_8 = table->find_row_bin(key_8, first_column);
    std::vector<char *> found_idk = table->find_row_bin(key_idk, first_column);

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

BOOST_AUTO_TEST_CASE(find_text_row_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::INT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    std::string key = "00000000000000000000000000000000";
    std::string num = "10";
    std::vector<std::string> row = {key, num, "2002-01-01"};
    for (int i = 0; i < 5; i++)
    {
        table->insert_row_text(row);
        key[PKEY_COL_W - 1]++;
        row[0] = key;
    }
    num = "8";
    row[1] = num;
    for (int i = 0; i < 5; i++)
    {
        table->insert_row_text(row);
        key[PKEY_COL_W - 1]++;
        row[0] = key;
    }
    key = "00000000000000000000000000000000";
    std::vector<char *> rows = table->find_row_text("10", "col_1");
    BOOST_CHECK(rows.size() == 5);
    for (size_t i = 0; i < rows.size(); i++)
    {
        int num = *reinterpret_cast<int *>(rows[i] + PKEY_COL_W);
        BOOST_CHECK(strncmp(rows[i], key.c_str(), PKEY_COL_W - 1) == 0);
        BOOST_CHECK(rows[i][PKEY_COL_W - 1] >= '0' && rows[i][PKEY_COL_W - 1] < '5');
        BOOST_CHECK(num == 10);
        BOOST_CHECK(strncmp(rows[i] + 48, "2002-01-01", 10) == 0);
    }
    for (size_t i = 0; i < rows.size(); i++)
    {
        delete[] rows[i];
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(load_table_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::INT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    std::string key = "00000000000000000000000000000000";
    std::string num = "10";
    std::vector<std::string> row = {key, num, "2002-01-01"};
    for (int i = 0; i < 5; i++)
    {
        table->insert_row_text(row);
        key[PKEY_COL_W - 1]++;
        row[0] = key;
    }
    std::memset(key.data(), '0', PKEY_COL_W);
    delete table;
    table = rsql::Table::load_table(db, "test_table");
    std::vector<char *> rows = table->find_row_text("00000000000000000000000000000000", "key");
    int row_num = *reinterpret_cast<int *>(rows[0] + PKEY_COL_W);
    BOOST_CHECK(rows.size() == 1);
    BOOST_CHECK(strncmp(rows[0], key.c_str(), PKEY_COL_W) == 0);
    BOOST_CHECK(row_num == 10);
    BOOST_CHECK(strncmp(rows[0] + 48, "2002-01-01", 10) == 0);
    delete rows[0];
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(optional_indexing_build_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::INT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::INT, 4));
    std::string key = "00000000000000000000000000000000";
    boost::multiprecision::cpp_int num_1 = 100000;
    std::string date = "10/02/2002";
    uint32_t num_2 = 0;
    char row[32 + 16 + 10 + 4];
    std::memcpy(row, key.data(), 32);
    std::memset(row + 32, 0, 16);
    boost::multiprecision::export_bits(num_1, row + 32, 8, false);
    std::memcpy(row + 48, date.data(), 10);
    std::memcpy(row + 58, &num_2, 4);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(row);
        num_2++;
        key[PKEY_COL_W - 1]++;
        std::memcpy(row, key.data(), 32);
        std::memcpy(row + 58, &num_2, 4);
    }
    table->index_column("col_3");
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(optional_indexing_find_quality_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::INT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::INT, 4));
    std::string key = "00000000000000000000000000000000";
    boost::multiprecision::cpp_int num_1 = 100000;
    std::string date = "10/02/2002";
    uint32_t num_2 = 0;
    char row[32 + 16 + 10 + 4];
    std::memcpy(row, key.data(), 32);
    std::memset(row + 32, 0, 16);
    boost::multiprecision::export_bits(num_1, row + 32, 8, false);
    std::memcpy(row + 48, date.data(), 10);
    std::memcpy(row + 58, &num_2, 4);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(row);
        if (i % 5 == 0)
        {
            num_2++;
        }
        key[PKEY_COL_W - 1]++;
        std::memcpy(row, key.data(), 32);
        std::memcpy(row + 58, &num_2, 4);
    }
    table->index_column("col_3");
    std::vector<char *> rows = table->find_row_text("1", "col_3");
    key = "00000000000000000000000000000001";
    uint32_t target_col_val = 1;
    std::memcpy(row, key.data(), 32);
    std::memset(row + 32, 0, 16);
    boost::multiprecision::export_bits(num_1, row + 32, 8, false);
    std::memcpy(row + 48, date.data(), 10);
    std::memcpy(row + 58, &target_col_val, 4);
    BOOST_CHECK(rows.size() == 5);
    for (size_t i = 0; i < rows.size(); i++)
    {
        BOOST_CHECK(std::strncmp(rows[i] + 32, row + 32, 30) == 0);
        BOOST_CHECK(rows[i][PKEY_COL_W - 1] >= '1');
        BOOST_CHECK(rows[i][PKEY_COL_W - 1] <= '5');
        std::memcpy(row, key.data(), 32);
        delete[] rows[i];
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(optional_indexing_find_quantity_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    std::string first_column = "key";
    table->add_column(first_column, rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::INT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::INT, 4));
    std::string key = "00000000000000000000000000000000";
    boost::multiprecision::cpp_int num_1 = 100000;
    std::string date = "10/02/2002";
    uint32_t num_2 = 0;
    char row[32 + 16 + 10 + 4];
    std::memcpy(row, key.data(), 32);
    std::memset(row + 32, 0, 16);
    boost::multiprecision::export_bits(num_1, row + 32, 8, false);
    std::memcpy(row + 48, date.data(), 10);
    std::memcpy(row + 58, &num_2, 4);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(row);
        key[PKEY_COL_W - 1]++;
        std::memcpy(row, key.data(), 32);
    }
    table->index_column("col_3");
    std::vector<char *> rows = table->find_row_text("0", "col_3");
    std::sort(rows.begin(), rows.end(), [](const char *r1, const char *r2)
              { return std::strncmp(r1, r2, 62) < 0; });
    row[PKEY_COL_W - 1] = '0';
    BOOST_CHECK(rows.size() == 50);
    for (size_t i = 0; i < rows.size(); i++)
    {
        BOOST_CHECK(std::strncmp(row, rows[i], 62) == 0);
        row[PKEY_COL_W - 1]++;
        delete[] rows[i];
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}