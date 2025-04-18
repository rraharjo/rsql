#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "database.h"

BOOST_AUTO_TEST_CASE(create_database_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    delete db;
    std::system("make cleancachefolder");
}

BOOST_AUTO_TEST_CASE(create_multiple_database_test)
{
    rsql::Database *db_1 = rsql::Database::create_new_database("test_db_1");
    rsql::Database *db_2 = rsql::Database::create_new_database("test_db_2");
    BOOST_CHECK(db_1 != nullptr);
    BOOST_CHECK(db_2 != nullptr);
    delete db_1;
    delete db_2;
    std::system("make cleancachefolder");
}
