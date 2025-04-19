#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "database.h"
std::string clear_cache = "make cleancachefolder";
BOOST_AUTO_TEST_CASE(create_database_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(create_multiple_database_test)
{
    rsql::Database *db_1 = rsql::Database::create_new_database("test_db_1");
    rsql::Database *db_2 = rsql::Database::create_new_database("test_db_2");
    BOOST_CHECK(db_1 != nullptr);
    BOOST_CHECK(db_2 != nullptr);
    delete db_1;
    delete db_2;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(create_database_already_exists_test)
{
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    BOOST_CHECK_THROW(rsql::Database::create_new_database("test_db"), std::invalid_argument);
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(remove_table_test){
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    rsql::Table *table = rsql::Table::create_new_table(db, "test_table");
    delete table;
    BOOST_CHECK(table != nullptr);
    db->remove_table("test_table");
    BOOST_CHECK_THROW(db->get_table("test_table"), std::invalid_argument);
    delete db;
    std::system(clear_cache.c_str());
}

BOOST_AUTO_TEST_CASE(remove_table_not_exists_test){
    rsql::Database *db = rsql::Database::create_new_database("test_db");
    BOOST_CHECK(db != nullptr);
    BOOST_CHECK_THROW(db->remove_table("test_table"), std::invalid_argument);
    delete db;
    std::system(clear_cache.c_str());
}