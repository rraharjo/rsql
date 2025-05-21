#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "sql_parser.h"
#include "database.h"
typedef boost::multiprecision::cpp_int cpp_int;
std::string clear_cache = "make cleancache";

BOOST_AUTO_TEST_CASE(insert_parser_test_extract_values){
    std::string values = "insert into some_table values  (asd, 132, 3 3, asds), (asdf, ddd, 333333333, 0x1f)  ";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "132", "3 3", "asds"}, {"asdf", "ddd", "333333333", "0x1f"}};
    rsql::InsertParser insert_parser(values);
    insert_parser.parse();

    BOOST_CHECK(insert_parser.get_table_name() == "some_table");
    BOOST_CHECK(insert_parser.row_values == expected_values);
}

BOOST_AUTO_TEST_CASE(insert_parser_test_values_with_quotes){
    std::string values = "insert into some_table values  (asd, \"13(( ))))))) some crazy values\", 3 3, asds), (asdf, ddd, \"   33333()()()3333\", 0x1f)  ";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "13(( ))))))) some crazy values", "3 3", "asds"}, {"asdf", "ddd", "   33333()()()3333", "0x1f"}};
    rsql::InsertParser insert_parser(values);
    insert_parser.parse();

    BOOST_CHECK(insert_parser.get_table_name() == "some_table");
    BOOST_CHECK(insert_parser.row_values == expected_values);
}

BOOST_AUTO_TEST_CASE(insert_parser_test_trailing_comma){
    std::string values = "insert into some_table values  (asd, 132, 3 3, asds), (asdf, ddd, 333333333, 0x1f),  ";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "132", "3 3", "asds"}, {"asdf", "ddd", "333333333", "0x1f"}};
    rsql::InsertParser insert_parser(values);

    BOOST_CHECK_THROW(insert_parser.parse(), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(insert_parser_test_opened_bracket){
    std::string values = "insert into some_table values  (asd, 132, 3 3, asds), (asdf, ddd, 333333333, dsdsd ";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "132", "3 3", "asds"}, {"asdf", "ddd", "333333333", "0x1f"}};
    rsql::InsertParser insert_parser(values);

    BOOST_CHECK_THROW(insert_parser.parse(), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(insert_parser_test_opened_quote){
    std::string values = "insert into some_table values  (asd, 132, 3 3, asds), (asdf, ddd, 333333333, \"dsdsd )";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "132", "3 3", "asds"}, {"asdf", "ddd", "333333333", "0x1f"}};
    rsql::InsertParser insert_parser(values);
    
    BOOST_CHECK_THROW(insert_parser.parse(), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(where_parser_test){
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);
    table->add_column("col_1", rsql::Column::get_column(0, rsql::DataType::UINT, 10));
    table->add_column("col_2", rsql::Column::get_column(0, rsql::DataType::DATE, 0));
    table->add_column("col_3", rsql::Column::get_column(0, rsql::DataType::SINT, 4));
    std::string condition = "where ((col_1 < 10 or col_1 == 10) and (col_2 == 2002/10/10 or col_3 == -100 ))";
    rsql::DeleteParser parser(condition);
    parser.extract_conditions(table);
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}