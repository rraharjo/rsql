#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "comparison.h"
typedef boost::multiprecision::cpp_int cpp_int;
std::vector<char *> get_sample_data();

BOOST_AUTO_TEST_CASE(constant_comparison_test)
{
    // def_key -> 32 bytes
    // uint -> 4 bytes
    std::vector<char *> sample_data;
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    cpp_int unsigned_cpp = 5000;
    for (int i = 0; i < 5; i++)
    {
        char *new_row = new char[36];
        std::memcpy(new_row, key, 32);
        boost::multiprecision::export_bits(unsigned_cpp, new_row + 32, 8, false);
        key[DEFAULT_KEY_WIDTH - 1]++;
        sample_data.push_back(new_row);
    }
    unsigned_cpp = 10000;
    for (int i = 0; i < 4; i++)
    {
        char *new_row = new char[36];
        std::memcpy(new_row, key, 32);
        boost::multiprecision::export_bits(unsigned_cpp, new_row + 32, 8, false);
        key[DEFAULT_KEY_WIDTH - 1]++;
        sample_data.push_back(new_row);
    }
    char constant_uint[4];
    boost::multiprecision::export_bits(unsigned_cpp, constant_uint, 8, false);
    rsql::ConstantComparison *uint_comparison = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, 4, 32, constant_uint);
    char constant_key[32];
    std::memset(constant_key, 0, 32);
    constant_key[31] = 4;
    rsql::ConstantComparison *key_comparison = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::LEQ, 32, 0, constant_key);
    size_t uint_correct = 0, key_correct = 0;
    for (char *row : sample_data)
    {
        if (uint_comparison->compare(row))
        {
            uint_correct++;
        }
        if (key_comparison->compare(row))
        {
            key_correct++;
        }
    }
    BOOST_CHECK(uint_correct == 4);
    BOOST_CHECK(key_correct == 5);
    delete uint_comparison;
    delete key_comparison;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(column_comparison)
{
    // def_key 32 bytes
    // sint    4 bytes
    // sint    8 bytes
    std::vector<char *> sample_data;
    cpp_int sint_1 = -5, sint_2 = 0;
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    for (int i = 0; i < 10; i++)
    {
        char *row = new char[44];
        std::memset(row, 0, 44);
        std::memcpy(row, key, DEFAULT_KEY_WIDTH);
        key[DEFAULT_KEY_WIDTH - 1]++;
        int sign_1, sign_2;
        sign_1 = sint_1.sign();
        sign_2 = sint_2.sign();
        std::memcpy(row + DEFAULT_KEY_WIDTH, &sign_1, 1);
        boost::multiprecision::export_bits(sint_1, row + DEFAULT_KEY_WIDTH + 1, 8, false);
        std::memcpy(row + 36, &sign_2, 1);
        boost::multiprecision::export_bits(sint_2, row + 37, 8, false);
        sample_data.push_back(row);
        sint_1 += 1;
        sint_2 += 1;
    }
    rsql::ColumnComparison *eq_comparison = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::EQ, 8, 36, 32, 4);
    rsql::ColumnComparison *leq_comparison = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LEQ, 4, 32, 36, 8);
    size_t eq_count = 0, leq_count = 0;
    for (char *c : sample_data)
    {
        if (eq_comparison->compare(c))
        {
            eq_count++;
        }
        if (leq_comparison->compare(c))
        {
            leq_count++;
        }
    }
    BOOST_CHECK(eq_count == 0);
    BOOST_CHECK(leq_count == 10);
    for (char *c : sample_data)
    {
        delete[] c;
    }
    delete eq_comparison;
    delete leq_comparison;
}