#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "sql_parser.h"

BOOST_AUTO_TEST_CASE(insert_parser_test_extract_values){
    std::string values = "insert into some_table values  (asd, 132, 3 3, asds), (asdf, ddd, 333333333, 0x1f)  ";
    std::vector<std::vector<std::string>> expected_values = {{"asd", "132", "3 3", "asds"}, {"asdf", "ddd", "333333333", "0x1f"}};
    rsql::InsertParser insert_parser(values);
    insert_parser.parse();

    BOOST_CHECK(insert_parser.get_table_name() == "some_table");
    BOOST_CHECK(insert_parser.row_values == expected_values);
}