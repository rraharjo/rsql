#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "database.h"

typedef boost::multiprecision::cpp_int cpp_int;
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

BOOST_AUTO_TEST_CASE(table_create_test_with_columns_test)
{
    std::vector<std::string> col_names = {"col_0", "col_1"};
    std::vector<rsql::Column> columns;
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
    BOOST_CHECK(table->get_width() == DEFAULT_KEY_WIDTH);
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    BOOST_CHECK(table->get_width() == DEFAULT_KEY_WIDTH + 10 + 10);
    table->remove_column("col_1");
    BOOST_CHECK(table->get_width() == DEFAULT_KEY_WIDTH + 10);
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

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));

    char row[] = "abcdefghij2002-01-01";
    for (int i = 0; i < 10; i++)
    {
        table->insert_row_bin(row);
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

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::UINT, 32));
    table->add_column("col_4", rsql::Column::get_column(0, rsql::DataType::SINT, 8));

    std::vector<std::string> row_1 = {"abcdefghij", "2002-01-01", "1234567890", "-10000"};
    std::vector<std::string> row_2 = {"klmnopqrst", "2002-01-12", "1000000000", "20000"};

    table->insert_row_text(row_1);
    table->insert_row_text(row_2);

    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);

    std::vector<char *> vec_row_1 = table->find_row_bin(key, DEF_KEY_COL_NAME);
    key[DEFAULT_KEY_WIDTH - 1]++;
    std::vector<char *> vec_row_2 = table->find_row_bin(key, DEF_KEY_COL_NAME);

    cpp_int uint1, uint2;
    boost::multiprecision::import_bits(uint1, vec_row_1[0] + 52, vec_row_1[0] + 52 + 32, 8, false);
    boost::multiprecision::import_bits(uint2, vec_row_2[0] + 52, vec_row_2[0] + 52 + 32, 8, false);
    char sign_byte_1 = *(vec_row_1[0] + 84);
    char sign_byte_2 = *(vec_row_2[0] + 84);
    int sign_1 = (int)sign_byte_1, sign_2 = (int)sign_byte_2;
    cpp_int signed_int_1, signed_int_2;
    boost::multiprecision::import_bits(signed_int_1, vec_row_1[0] + 85, vec_row_1[0] + 92, 8, false);
    boost::multiprecision::import_bits(signed_int_2, vec_row_2[0] + 85, vec_row_2[0] + 92, 8, false);
    signed_int_1 *= sign_1;
    signed_int_2 *= sign_2;

    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    BOOST_CHECK(vec_row_1.size() == 1);
    BOOST_CHECK(strncmp(vec_row_1[0], key, 32) == 0);
    BOOST_CHECK(strncmp(vec_row_1[0] + 32, row_1[0].c_str(), 10) == 0);
    BOOST_CHECK(strncmp(vec_row_1[0] + 42, row_1[1].c_str(), 10) == 0);
    BOOST_CHECK(uint1 == 1234567890);
    BOOST_CHECK(signed_int_1 == cpp_int("-10000"));
    key[DEFAULT_KEY_WIDTH - 1] = 1;
    BOOST_CHECK(vec_row_2.size() == 1);
    BOOST_CHECK(strncmp(vec_row_2[0], key, 32) == 0);
    BOOST_CHECK(strncmp(vec_row_2[0] + 32, row_2[0].c_str(), 10) == 0);
    BOOST_CHECK(strncmp(vec_row_2[0] + 42, row_2[1].c_str(), 10) == 0);
    BOOST_CHECK(uint2 == 1000000000);
    BOOST_CHECK(signed_int_2 == cpp_int("20000"));

    delete[] vec_row_1[0];
    delete[] vec_row_2[0];
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(insert_row_exceeding_column_size_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::UINT, 32));
    table->add_column("col_4", rsql::Column::get_column(0, rsql::DataType::SINT, 4));

    std::vector<std::string> row_1 = {"abcdefghij", "2002-01-01", "1234567890", "-16777216"}; // 0x01 00 00 00
    std::vector<std::string> row_2 = {"klmnopqrst", "2002-01-01", "1000000000", "-16777215"}; // 0xff ff ff

    BOOST_CHECK_THROW(table->insert_row_text(row_1), std::invalid_argument);
    table->insert_row_text(row_2);

    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    std::vector<char *> vec_row_2 = table->find_row_bin(key, DEF_KEY_COL_NAME);

    cpp_int unsigned_int;
    boost::multiprecision::import_bits(unsigned_int, vec_row_2[0] + 52, vec_row_2[0] + 84, 8, false);
    char sign_byte_2 = *(vec_row_2[0] + 84);
    int sign_2 = (int)sign_byte_2;
    cpp_int signed_int_2;
    boost::multiprecision::import_bits(signed_int_2, vec_row_2[0] + 85, vec_row_2[0] + 88, 8, false);
    signed_int_2 *= sign_2;
    BOOST_CHECK(vec_row_2.size() == 1);
    BOOST_CHECK(strncmp(vec_row_2[0], key, DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(strncmp(vec_row_2[0] + 32, row_2[0].c_str(), 10) == 0);
    BOOST_CHECK(strncmp(vec_row_2[0] + 42, row_2[1].c_str(), 10) == 0);
    BOOST_CHECK(unsigned_int == 1000000000);
    BOOST_CHECK(signed_int_2 == cpp_int("-16777215"));
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

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));

    char row[] = "abcdefghij2002-01-01";
    for (int i = 0; i < 10; i++)
    {
        table->insert_row_bin(row);
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    std::vector<char *> found_0 = table->find_row_bin(key, DEF_KEY_COL_NAME);
    key[DEFAULT_KEY_WIDTH - 1] = 5;
    std::vector<char *> found_5 = table->find_row_bin(key, DEF_KEY_COL_NAME);
    key[DEFAULT_KEY_WIDTH - 1] = 8;
    std::vector<char *> found_8 = table->find_row_bin(key, DEF_KEY_COL_NAME);
    key[DEFAULT_KEY_WIDTH - 1] = 10;
    std::vector<char *> found_idk = table->find_row_bin(key, DEF_KEY_COL_NAME);

    BOOST_CHECK(found_0.size() == 1);
    BOOST_CHECK(found_5.size() == 1);
    BOOST_CHECK(found_8.size() == 1);
    BOOST_CHECK(found_idk.size() == 0);
    key[DEFAULT_KEY_WIDTH - 1] = 0;
    BOOST_CHECK(strncmp(found_0[0], key, DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(strncmp(found_0[0] + DEFAULT_KEY_WIDTH, row, 20) == 0);
    key[DEFAULT_KEY_WIDTH - 1] = 5;
    BOOST_CHECK(strncmp(found_5[0], key, DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(strncmp(found_5[0] + DEFAULT_KEY_WIDTH, row, 20) == 0);
    key[DEFAULT_KEY_WIDTH - 1] = 8;
    BOOST_CHECK(strncmp(found_8[0], key, DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(strncmp(found_8[0] + DEFAULT_KEY_WIDTH, row, 20) == 0);

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

    table->add_column("col_0", rsql::Column::get_column(0, rsql::DataType::SINT, 8));
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    std::string key = "-10000";
    std::string num = "10";
    std::vector<std::string> row = {key, num, "2002-01-01"};
    for (int i = 0; i < 5; i++)
    {
        table->insert_row_text(row);
        key[5]++;
        row[0] = key;
    }
    num = "8";
    row[1] = num;
    for (int i = 0; i < 5; i++)
    {
        table->insert_row_text(row);
        key[5]++;
        row[0] = key;
    }
    std::vector<char *> rows = table->find_row_text("10", "col_1");
    BOOST_CHECK(rows.size() == 5);
    for (size_t i = 0; i < rows.size(); i++)
    {
        int sign = (int)(*(rows[i] + 32));
        cpp_int magnitude;
        boost::multiprecision::import_bits(magnitude, rows[i] + 33, rows[i] + 40, 8, false);
        magnitude *= sign;
        int num = *reinterpret_cast<int *>(rows[i] + 40);
        BOOST_CHECK(magnitude <= -10000);
        BOOST_CHECK(magnitude >= -10004);
        BOOST_CHECK(num == 10);
        BOOST_CHECK(strncmp(rows[i] + 50, "2002-01-01", 10) == 0);
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

    char key[DEFAULT_KEY_WIDTH];
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    std::string num = "10";
    std::vector<std::string> row = {num, "2002-01-01"};
    for (int i = 0; i < 5; i++)
    {
        table->insert_row_text(row);
    }
    delete table;
    table = rsql::Table::load_table(db, "test_table");
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    std::vector<char *> rows = table->find_row_text(key, DEF_KEY_COL_NAME);
    cpp_int row_num;
    boost::multiprecision::import_bits(row_num, rows[0] + DEFAULT_KEY_WIDTH, rows[0] + DEFAULT_KEY_WIDTH + 10, 8, false);
    BOOST_CHECK(rows.size() == 1);
    BOOST_CHECK(strncmp(rows[0], key, DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(row_num == 10);
    BOOST_CHECK(strncmp(rows[0] + 42, "2002-01-01", 10) == 0);
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

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::UINT, 4));
    boost::multiprecision::cpp_int num_1 = 100000;
    std::string date = "10/02/2002";
    uint32_t num_2 = 0;
    char row[10 + 10 + 4];
    std::memset(row, 0, 10);
    boost::multiprecision::export_bits(num_1, row, 8, false);
    std::memcpy(row + 10, date.data(), 10);
    std::memcpy(row + 20, &num_2, 4);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(row);
        num_2++;
        std::memcpy(row + 20, &num_2, 4);
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

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::UINT, 4));
    boost::multiprecision::cpp_int num_1 = 100000;
    std::string date = "10/02/2002";
    uint32_t num_2 = 0;
    char row[10 + 10 + 4];
    std::memset(row, 0, 10);
    boost::multiprecision::export_bits(num_1, row, 8, false);
    std::memcpy(row + 10, date.data(), 10);
    std::memcpy(row + 20, &num_2, 4);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(row);
        if (i % 5 == 0)
        {
            num_2++;
        }
        std::memcpy(row + 20, &num_2, 4);
    }
    table->index_column("col_3");
    std::vector<char *> rows = table->find_row_text("1", "col_3");
    uint32_t target_col_val = 1;

    std::memset(row, 0, 10);
    boost::multiprecision::export_bits(num_1, row, 8, false);
    std::memcpy(row + 10, date.data(), 10);
    std::memcpy(row + 20, &target_col_val, 4);
    BOOST_CHECK(rows.size() == 5);
    for (size_t i = 0; i < rows.size(); i++)
    {
        BOOST_CHECK(std::strncmp(rows[i] + 32, row, 24) == 0);
        BOOST_CHECK(rows[i][DEFAULT_KEY_WIDTH - 1] >= 1);
        BOOST_CHECK(rows[i][DEFAULT_KEY_WIDTH - 1] <= 5);
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

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::UINT, 4));
    boost::multiprecision::cpp_int num_1 = 100000;
    std::string date = "10/02/2002";
    uint32_t num_2 = 0;
    char row[10 + 10 + 4];
    std::memset(row, 0, 10);
    boost::multiprecision::export_bits(num_1, row, 8, false);
    std::memcpy(row + 10, date.data(), 10);
    std::memcpy(row + 20, &num_2, 4);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(row);
    }
    table->index_column("col_3");
    std::vector<char *> rows = table->find_row_text("0", "col_3");
    std::sort(rows.begin(), rows.end(), [](const char *r1, const char *r2)
              { return std::strncmp(r1, r2, 56) < 0; });
    BOOST_CHECK(rows.size() == 50);
    char result_row[32 + 10 + 10 + 4];
    std::memset(result_row, 0, 56);
    boost::multiprecision::export_bits(num_1, result_row + DEFAULT_KEY_WIDTH, 8, false);
    std::memcpy(result_row + DEFAULT_KEY_WIDTH + 10, date.data(), 10);
    std::memcpy(result_row + DEFAULT_KEY_WIDTH + 20, &num_2, 4);
    for (size_t i = 0; i < rows.size(); i++)
    {
        BOOST_CHECK(std::strncmp(result_row, rows[i], 56) == 0);
        result_row[DEFAULT_KEY_WIDTH - 1]++;
        delete[] rows[i];
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(optional_indexing_column_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::SINT, 4));

    char row[10 + 10 + 4];
    cpp_int ucpp_int = 1234567890;
    std::memset(row, 0, 24);
    boost::multiprecision::export_bits(ucpp_int, row, 8, false);
    std::memcpy(row + 10, "2002-12-12", 10);
    cpp_int scpp_int = -4321;
    uint8_t sign = static_cast<uint8_t>(scpp_int.sign());
    std::memcpy(row + 20, &sign, 1);
    boost::multiprecision::export_bits(scpp_int, row + 21, 8, false);
    for (int i = 0; i < 25; i++)
    {
        table->insert_row_bin(row);
        row[0]++;
    }
    table->index_column("col_1");
    table->index_column("col_2");
    table->index_column("col_3");
    for (int i = 0; i < 25; i++)
    {
        table->insert_row_bin(row);
        row[0]++;
    }
    std::vector<char *> res_col_2 = table->find_row_text("2002-12-12", "col_2");
    std::vector<char *> res_col_3 = table->find_row_text("-4321", "col_3");
    BOOST_CHECK(res_col_2.size() == 50);
    BOOST_CHECK(res_col_3.size() == 50);
    for (const char *c : res_col_2)
    {
        delete[] c;
    }
    for (const char *c : res_col_3)
    {
        delete[] c;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(optional_indexing_delete_column_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::SINT, 4));

    char row[10 + 10 + 4];
    std::memset(row, 0, 24);
    cpp_int ucpp_int = 1234567890;
    std::memset(row, 0, 10);
    boost::multiprecision::export_bits(ucpp_int, row, 8, false);
    std::memcpy(row + 10, "2002-12-12", 10);
    cpp_int scpp_int = -4321;
    uint8_t sign = static_cast<uint8_t>(scpp_int.sign());
    std::memcpy(row + 20, &sign, 1);
    boost::multiprecision::export_bits(scpp_int, row + 21, 8, false);
    table->index_column("col_1");
    table->index_column("col_2");
    table->index_column("col_3");
    for (int i = 0; i < 25; i++)
    {
        table->insert_row_bin(row);
        row[0]++;
    }
    table->remove_column("col_2");
    table->remove_column("col_1");
    std::memset(row, 0, 4);
    std::memcpy(row, &sign, 1);
    boost::multiprecision::export_bits(scpp_int, row + 1, 8, false);
    for (int i = 0; i < 25; i++)
    {
        table->insert_row_bin(row);
    }
    std::vector<char *> res_col_3 = table->find_row_text("-4321", "col_3");
    BOOST_CHECK(res_col_3.size() == 50);
    for (const char *c : res_col_3)
    {
        delete[] c;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(optional_indexing_write_reload_table_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::SINT, 4));

    char row[10 + 10 + 4];
    std::memset(row, 0, 24);
    cpp_int ucpp_int = 1234567890;
    std::memset(row, 0, 10);
    boost::multiprecision::export_bits(ucpp_int, row, 8, false);
    std::memcpy(row + 10, "2002-12-12", 10);
    cpp_int scpp_int = -4321;
    uint8_t sign = static_cast<uint8_t>(scpp_int.sign());
    std::memcpy(row + 20, &sign, 1);
    boost::multiprecision::export_bits(scpp_int, row + 21, 8, false);
    table->index_column("col_1");
    table->index_column("col_2");
    table->index_column("col_3");
    for (int i = 0; i < 25; i++)
    {
        table->insert_row_bin(row);
        row[0]++;
    }
    table->remove_column("col_2");
    table->remove_column("col_1");
    std::memset(row, 0, 4);
    std::memcpy(row, &sign, 1);
    boost::multiprecision::export_bits(scpp_int, row + 1, 8, false);

    delete table;
    table = rsql::Table::load_table(db, "test_table");

    for (int i = 0; i < 25; i++)
    {
        table->insert_row_bin(row);
    }
    std::vector<char *> res_col_3 = table->find_row_text("-4321", "col_3");
    BOOST_CHECK(res_col_3.size() == 50);
    for (const char *c : res_col_3)
    {
        delete[] c;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(search_table_test_primary_tree)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::SINT, 4));

    const size_t buff_width = 10 + 10 + 4;
    char buff[buff_width];
    std::memset(buff, 0, buff_width);
    cpp_int unsigned_int = 100000;
    rsql::ucpp_int_to_char(buff, 10, unsigned_int);
    std::memcpy(buff + 10, "2025-05-12", 10);
    cpp_int signed_int = -15000;
    rsql::scpp_int_to_char(buff + 20, 4, signed_int);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(buff);
        unsigned_int++;
        rsql::ucpp_int_to_char(buff, 10, unsigned_int);
    }
    char search_key[DEFAULT_KEY_WIDTH];
    std::memset(search_key, 0, DEFAULT_KEY_WIDTH);
    search_key[DEFAULT_KEY_WIDTH - 1] = 37;
    std::vector<char *> res_indexed = table->search_row_single_key(DEF_KEY_COL_NAME, search_key, rsql::CompSymbol::LEQ, nullptr);

    char uint_search[10];
    cpp_int uint_search_int = 100005;
    rsql::ucpp_int_to_char(uint_search, 10, uint_search_int);
    rsql::Comparison *comp = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::GEQ, 10, DEFAULT_KEY_WIDTH, uint_search);
    std::vector<char *> res_with_comparison = table->search_row_single_key(DEF_KEY_COL_NAME, search_key, rsql::CompSymbol::LEQ, comp);

    rsql::Comparison *key_comparison = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::LEQ, DEFAULT_KEY_WIDTH, 0, search_key);
    std::vector<char *> res_linear = table->search_row_single_key("col_1", uint_search, rsql::CompSymbol::GEQ, key_comparison);

    delete key_comparison;
    delete comp;

    BOOST_CHECK(res_indexed.size() == 38);
    for (size_t i = 0; i < res_indexed.size(); i++)
    {
        BOOST_CHECK(res_indexed[i][DEFAULT_KEY_WIDTH - 1] >= 0);
        BOOST_CHECK(res_indexed[i][DEFAULT_KEY_WIDTH - 1] <= 37);
        cpp_int this_row_uint;
        rsql::char_to_ucpp_int(res_indexed[i] + DEFAULT_KEY_WIDTH, 10, this_row_uint);
        BOOST_CHECK(this_row_uint >= 100000);
        BOOST_CHECK(this_row_uint <= 100037);
        BOOST_CHECK(std::strncmp(res_indexed[i] + DEFAULT_KEY_WIDTH + 10, "2025-05-12", 10) == 0);
        cpp_int this_row_sint;
        rsql::char_to_scpp_int(res_indexed[i] + DEFAULT_KEY_WIDTH + 20, 4, this_row_sint);
        BOOST_CHECK(this_row_sint == -15000);
    }

    BOOST_CHECK(res_with_comparison.size() == 33);
    for (size_t i = 0; i < res_with_comparison.size(); i++)
    {
        BOOST_CHECK(res_with_comparison[i][DEFAULT_KEY_WIDTH - 1] >= 5);
        BOOST_CHECK(res_with_comparison[i][DEFAULT_KEY_WIDTH - 1] <= 37);
        cpp_int this_row_uint;
        rsql::char_to_ucpp_int(res_with_comparison[i] + DEFAULT_KEY_WIDTH, 10, this_row_uint);
        BOOST_CHECK(this_row_uint >= 100005);
        BOOST_CHECK(this_row_uint <= 100037);
        BOOST_CHECK(std::strncmp(res_with_comparison[i] + DEFAULT_KEY_WIDTH + 10, "2025-05-12", 10) == 0);
        cpp_int this_row_sint;
        rsql::char_to_scpp_int(res_with_comparison[i] + DEFAULT_KEY_WIDTH + 20, 4, this_row_sint);
        BOOST_CHECK(this_row_sint == -15000);
    }

    BOOST_CHECK(res_linear.size() == 33);
    for (size_t i = 0; i < res_linear.size(); i++)
    {
        BOOST_CHECK(res_linear[i][DEFAULT_KEY_WIDTH - 1] >= 5);
        BOOST_CHECK(res_linear[i][DEFAULT_KEY_WIDTH - 1] <= 37);
        cpp_int this_row_uint;
        rsql::char_to_ucpp_int(res_linear[i] + DEFAULT_KEY_WIDTH, 10, this_row_uint);
        BOOST_CHECK(this_row_uint >= 100005);
        BOOST_CHECK(this_row_uint <= 100037);
        BOOST_CHECK(std::strncmp(res_linear[i] + DEFAULT_KEY_WIDTH + 10, "2025-05-12", 10) == 0);
        cpp_int this_row_sint;
        rsql::char_to_scpp_int(res_linear[i] + DEFAULT_KEY_WIDTH + 20, 4, this_row_sint);
        BOOST_CHECK(this_row_sint == -15000);
    }
    for (const char *r : res_indexed)
    {
        delete[] r;
    }
    for (const char *r : res_with_comparison)
    {
        delete[] r;
    }
    for (const char *r : res_linear)
    {
        delete[] r;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(search_table_test_optional_tree)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);

    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::SINT, 4));
    table->index_column("col_1");
    const size_t buff_width = 10 + 10 + 4;
    char buff[buff_width];
    std::memset(buff, 0, buff_width);
    cpp_int unsigned_int = 100000;
    rsql::ucpp_int_to_char(buff, 10, unsigned_int);
    std::memcpy(buff + 10, "2025-05-12", 10);
    cpp_int signed_int = -15000;
    rsql::scpp_int_to_char(buff + 20, 4, signed_int);
    for (size_t i = 0; i < 50; i++)
    {
        table->insert_row_bin(buff);
        unsigned_int++;
        rsql::ucpp_int_to_char(buff, 10, unsigned_int);
    }
    cpp_int search_uint = 100010;
    char uint_buff[10];
    rsql::ucpp_int_to_char(uint_buff, 10, search_uint);
    std::vector<char *> res = table->search_row_single_key("col_1", uint_buff, rsql::CompSymbol::GEQ, nullptr);
    BOOST_CHECK(res.size() == 40);
    for (const char *row : res)
    {
        BOOST_CHECK(row[DEFAULT_KEY_WIDTH - 1] >= 10);
        BOOST_CHECK(row[DEFAULT_KEY_WIDTH - 1] <= 49);
        cpp_int cur_uint;
        rsql::char_to_ucpp_int((char *)row + DEFAULT_KEY_WIDTH, 10, cur_uint);
        BOOST_CHECK(cur_uint >= 100010);
        BOOST_CHECK(cur_uint <= 100049);
        BOOST_CHECK(std::strncmp((char *)row + DEFAULT_KEY_WIDTH + 10, "2025-05-12", 10) == 0);
        cpp_int cur_sint;
        rsql::char_to_scpp_int((char *)row + DEFAULT_KEY_WIDTH + 20, 4, cur_sint);
        BOOST_CHECK(cur_sint == -15000);
    }
    for (const char *row : res)
    {
        delete[] row;
    }
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}
