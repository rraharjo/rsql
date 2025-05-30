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

    BOOST_CHECK(insert_parser.get_target_name() == "some_table");
    BOOST_CHECK(insert_parser.row_values == expected_values);
}

BOOST_AUTO_TEST_CASE(insert_parser_test_values_with_quotes){
    std::string values = "insert into some_table values  (asd, \"13(( ))))))) some crazy values\", 3 3, asds), (asdf, ddd, \"   33333()()()3333\", 0x1f)  ";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "\"13(( ))))))) some crazy values\"", "3 3", "asds"}, {"asdf", "ddd", "\"   33333()()()3333\"", "0x1f"}};
    rsql::InsertParser insert_parser(values);
    insert_parser.parse();

    BOOST_CHECK(insert_parser.get_target_name() == "some_table");
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
    table->add_column("col_4", rsql::Column::get_column(0, rsql::DataType::SINT, 4));
    std::string condition = "delete from test_table where col_3 > 1000 and ((col_1 < 10 or col_1 == 10) and (col_2 == \"2002-10-10\" or col_3 == -100 or col_3 < col_4 ))";
    rsql::DeleteParser parser(condition);
    parser.parse();
    parser.extract_comparisons(table);
    cpp_int first_val = 10;
    cpp_int second_val = 10;
    cpp_int fourth_val = -100;
    cpp_int main_val = 1000;
    char first_val_char[10];
    char second_val_char[10];
    char third_val_char[10];
    char fourth_val_char[4];
    char main_val_char[4];

    rsql::ucpp_int_to_char(first_val_char, 10, first_val);
    rsql::ucpp_int_to_char(second_val_char, 10, second_val);
    std::memcpy(third_val_char, "2002-10-10", 10);
    rsql::scpp_int_to_char(fourth_val_char, 4, fourth_val);
    rsql::scpp_int_to_char(main_val_char, 4, main_val);

    rsql::MultiComparisons *optional_comparison = new rsql::ANDComparisons();
    rsql::MultiComparisons *left_comparison = new rsql::ORComparisons();
    rsql::MultiComparisons *right_comparison = new rsql::ORComparisons();
    rsql::Comparison *first_comp = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::LT, 10, 32, first_val_char);
    rsql::Comparison *second_comp = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, 10, 32, second_val_char);
    rsql::Comparison *third_comp = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::EQ, 10, 42, third_val_char);
    rsql::Comparison *fourth_comp = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::EQ, 4, 52, fourth_val_char);
    rsql::Comparison *fifth_comp = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LT, 4, 52, 4, 56);
    left_comparison->add_condition(first_comp);
    left_comparison->add_condition(second_comp);
    right_comparison->add_condition(third_comp);
    right_comparison->add_condition(fourth_comp);
    right_comparison->add_condition(fifth_comp);
    optional_comparison->add_condition(left_comparison);
    optional_comparison->add_condition(right_comparison);
    BOOST_CHECK(parser.get_main_col_name() == "col_3");
    BOOST_CHECK(parser.get_main_symbol() == rsql::CompSymbol::GT);
    BOOST_CHECK(std::memcmp(parser.get_main_val(), main_val_char, 4) == 0);
    BOOST_CHECK(*(parser.get_comparison()) == *optional_comparison);
    delete left_comparison;
    delete right_comparison;
    delete first_comp;
    delete second_comp;
    delete third_comp;
    delete fourth_comp;
    delete fifth_comp;
    delete optional_comparison;
    delete table;
    delete db;
    std::system(clear_cache.c_str());
}