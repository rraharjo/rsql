#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "database.h"

BOOST_AUTO_TEST_CASE(table_create_test)
{
    std::system("make cleandbfolder");
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    BOOST_CHECK(table != nullptr);
    delete table;
    delete db;
}