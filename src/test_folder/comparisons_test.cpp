#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "comparison.h"
typedef boost::multiprecision::cpp_int cpp_int;

BOOST_AUTO_TEST_CASE(constant_comparison_test_default_key)
{
    // def_key -> 32 bytes
    std::vector<char *> sample_data;
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    for (int i = 0; i < 5; i++) // 0 to 4
    {
        char *new_row = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(new_row, key, DEFAULT_KEY_WIDTH);
        rsql::increment_default_key((unsigned char *)key);
        sample_data.push_back(new_row);
    }
    for (int i = 0; i < 4; i++) // 5
    {
        char *new_row = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(new_row, key, DEFAULT_KEY_WIDTH);
        sample_data.push_back(new_row);
    }
    for (int i = 0; i < 4; i++) // 6 to 9
    {
        rsql::increment_default_key((unsigned char *)key);
        char *new_row = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(new_row, key, DEFAULT_KEY_WIDTH);
        sample_data.push_back(new_row);
    }
    char constant_key[DEFAULT_KEY_WIDTH];
    std::memset(constant_key, 0, DEFAULT_KEY_WIDTH);
    constant_key[DEFAULT_KEY_WIDTH - 1] = 4;
    rsql::ConstantComparison *lt = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::LT, DEFAULT_KEY_WIDTH, 0, constant_key);
    rsql::ConstantComparison *leq = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::LEQ, DEFAULT_KEY_WIDTH, 0, constant_key);
    rsql::ConstantComparison *eq = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::EQ, DEFAULT_KEY_WIDTH, 0, constant_key);
    rsql::ConstantComparison *geq = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::GEQ, DEFAULT_KEY_WIDTH, 0, constant_key);
    rsql::ConstantComparison *gt = new rsql::ConstantComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::GT, DEFAULT_KEY_WIDTH, 0, constant_key);
    size_t lt_correct = 0, leq_correct = 0, eq_correct = 0, geq_correct = 0, gt_correct = 0;
    for (char *row : sample_data)
    {
        if (lt->compare(row))
        {
            lt_correct++;
        }
        if (leq->compare(row))
        {
            leq_correct++;
        }
        if (eq->compare(row))
        {
            eq_correct++;
        }
        if (geq->compare(row))
        {
            geq_correct++;
        }
        if (gt->compare(row))
        {
            gt_correct++;
        }
    }
    BOOST_CHECK(lt_correct == 4);
    BOOST_CHECK(leq_correct == 5);
    BOOST_CHECK(eq_correct == 1);
    BOOST_CHECK(geq_correct == 9);
    BOOST_CHECK(gt_correct == 8);
    delete lt;
    delete leq;
    delete eq;
    delete geq;
    delete gt;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(constant_comparison_test_char)
{
    // CHAR -> 10 bytes
    std::vector<char *> sample_data;
    const size_t width = 10;
    char the_char[width];
    std::memset(the_char, 'a', width);
    for (int i = 0; i < 5; i++) // a to e
    {
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        std::memcpy(new_row, the_char, width);
        sample_data.push_back(new_row);
        the_char[width - 1]++;
    }
    for (int i = 0; i < 4; i++) // f
    {
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        std::memcpy(new_row, the_char, width);
        sample_data.push_back(new_row);
    }
    for (int i = 0; i < 4; i++) // g to j
    {
        the_char[width - 1]++;
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        std::memcpy(new_row, the_char, width);
        sample_data.push_back(new_row);
    }
    char constant_char[width];
    std::memset(constant_char, 0, width);
    std::memcpy(constant_char, "aaaaaaaaaj", width);
    rsql::ConstantComparison *lt = new rsql::ConstantComparison(rsql::DataType::CHAR, rsql::CompSymbol::LT, width, 0, constant_char);
    rsql::ConstantComparison *leq = new rsql::ConstantComparison(rsql::DataType::CHAR, rsql::CompSymbol::LEQ, width, 0, constant_char);
    rsql::ConstantComparison *eq = new rsql::ConstantComparison(rsql::DataType::CHAR, rsql::CompSymbol::EQ, width, 0, constant_char);
    rsql::ConstantComparison *geq = new rsql::ConstantComparison(rsql::DataType::CHAR, rsql::CompSymbol::GEQ, width, 0, constant_char);
    rsql::ConstantComparison *gt = new rsql::ConstantComparison(rsql::DataType::CHAR, rsql::CompSymbol::GT, width, 0, constant_char);
    size_t lt_correct = 0, leq_correct = 0, eq_correct = 0, geq_correct = 0, gt_correct = 0;
    for (char *row : sample_data)
    {
        if (lt->compare(row))
        {
            lt_correct++;
        }
        if (leq->compare(row))
        {
            leq_correct++;
        }
        if (eq->compare(row))
        {
            eq_correct++;
        }
        if (geq->compare(row))
        {
            geq_correct++;
        }
        if (gt->compare(row))
        {
            gt_correct++;
        }
    }
    BOOST_CHECK(lt_correct == 12);
    BOOST_CHECK(leq_correct == 13);
    BOOST_CHECK(eq_correct == 1);
    BOOST_CHECK(geq_correct == 1);
    BOOST_CHECK(gt_correct == 0);
    delete lt;
    delete leq;
    delete eq;
    delete geq;
    delete gt;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(constant_comparison_test_date)
{
    // DATE -> 10 bytes
    std::vector<char *> sample_data;
    const size_t width = 10;
    char *date_row;

    date_row = new char[width];
    std::memcpy(date_row, "2002-01-01", 10);
    sample_data.push_back(date_row);

    date_row = new char[width];
    std::memcpy(date_row, "2002-01-02", 10);
    sample_data.push_back(date_row);

    date_row = new char[width];
    std::memcpy(date_row, "2002-02-01", 10);
    sample_data.push_back(date_row);

    date_row = new char[width];
    std::memcpy(date_row, "2003-01-01", 10);
    sample_data.push_back(date_row);

    for (int i = 0; i < 4; i++) // f
    {
        date_row = new char[width];
        std::memcpy(date_row, "2004-01-01", 10);
        sample_data.push_back(date_row);
    }
    char constant_char[width];
    std::memcpy(constant_char, "2002-12-01", width);
    rsql::ConstantComparison *lt = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::LT, width, 0, constant_char);
    rsql::ConstantComparison *leq = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::LEQ, width, 0, constant_char);
    rsql::ConstantComparison *eq = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::EQ, width, 0, constant_char);
    rsql::ConstantComparison *geq = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::GEQ, width, 0, constant_char);
    rsql::ConstantComparison *gt = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::GT, width, 0, constant_char);
    size_t lt_correct = 0, leq_correct = 0, eq_correct = 0, geq_correct = 0, gt_correct = 0;
    for (char *row : sample_data)
    {
        if (lt->compare(row))
        {
            lt_correct++;
        }
        if (leq->compare(row))
        {
            leq_correct++;
        }
        if (eq->compare(row))
        {
            eq_correct++;
        }
        if (geq->compare(row))
        {
            geq_correct++;
        }
        if (gt->compare(row))
        {
            gt_correct++;
        }
    }
    BOOST_CHECK(lt_correct == 3);
    BOOST_CHECK(leq_correct == 3);
    BOOST_CHECK(eq_correct == 0);
    BOOST_CHECK(geq_correct == 5);
    BOOST_CHECK(gt_correct == 5);
    delete lt;
    delete leq;
    delete eq;
    delete geq;
    delete gt;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(constant_comparison_test_unsigned_int)
{
    // UINT -> 4 bytes
    std::vector<char *> sample_data;
    size_t width = 4;
    cpp_int the_int = 10;
    for (int i = 0; i < 5; i++) // 10 to 14
    {
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        boost::multiprecision::export_bits(the_int, new_row, 8, false);
        sample_data.push_back(new_row);
        the_int++;
    }
    for (int i = 0; i < 4; i++) // 15
    {
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        boost::multiprecision::export_bits(the_int, new_row, 8, false);
        sample_data.push_back(new_row);
    }
    for (int i = 0; i < 4; i++) // 16 to 19
    {
        the_int++;
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        boost::multiprecision::export_bits(the_int, new_row, 8, false);
        sample_data.push_back(new_row);
    }
    cpp_int constant_int = 15;
    char constant_char[width];
    std::memset(constant_char, 0, width);
    boost::multiprecision::export_bits(constant_int, constant_char, 8, false);
    rsql::ConstantComparison *lt = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::LT, width, 0, constant_char);
    rsql::ConstantComparison *leq = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::LEQ, width, 0, constant_char);
    rsql::ConstantComparison *eq = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, width, 0, constant_char);
    rsql::ConstantComparison *geq = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::GEQ, width, 0, constant_char);
    rsql::ConstantComparison *gt = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::GT, width, 0, constant_char);
    size_t lt_correct = 0, leq_correct = 0, eq_correct = 0, geq_correct = 0, gt_correct = 0;
    for (char *row : sample_data)
    {
        if (lt->compare(row))
        {
            lt_correct++;
        }
        if (leq->compare(row))
        {
            leq_correct++;
        }
        if (eq->compare(row))
        {
            eq_correct++;
        }
        if (geq->compare(row))
        {
            geq_correct++;
        }
        if (gt->compare(row))
        {
            gt_correct++;
        }
    }
    BOOST_CHECK(lt_correct == 5);
    BOOST_CHECK(leq_correct == 9);
    BOOST_CHECK(eq_correct == 4);
    BOOST_CHECK(geq_correct == 8);
    BOOST_CHECK(gt_correct == 4);
    delete lt;
    delete leq;
    delete eq;
    delete geq;
    delete gt;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(constant_comparison_test_signed_int)
{
    // UINT -> 4 bytes
    std::vector<char *> sample_data;
    size_t width = 4; // 1 byte sign, 3 bytes magnitude
    cpp_int the_int = -5;
    for (int i = 0; i < 5; i++) // -5 to -1
    {
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        int sign = the_int.sign();
        std::memcpy(new_row, &sign, 1);
        boost::multiprecision::export_bits(the_int, new_row + 1, 8, false);
        sample_data.push_back(new_row);
        the_int++;
    }
    for (int i = 0; i < 4; i++) // 0
    {
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        int sign = the_int.sign();
        std::memcpy(new_row, &sign, 1);
        boost::multiprecision::export_bits(the_int, new_row + 1, 8, false);
        sample_data.push_back(new_row);
    }
    for (int i = 0; i < 4; i++) // 1 to 4;
    {
        the_int++;
        char *new_row = new char[width];
        std::memset(new_row, 0, width);
        int sign = the_int.sign();
        std::memcpy(new_row, &sign, 1);
        boost::multiprecision::export_bits(the_int, new_row + 1, 8, false);
        sample_data.push_back(new_row);
    }
    cpp_int constant_int = -1;
    int constant_sign = constant_int.sign();
    char constant_char[width];
    std::memset(constant_char, 0, width);
    std::memcpy(constant_char, &constant_sign, 1);
    boost::multiprecision::export_bits(constant_int, constant_char + 1, 8, false);
    rsql::ConstantComparison *lt = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::LT, width, 0, constant_char);
    rsql::ConstantComparison *leq = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::LEQ, width, 0, constant_char);
    rsql::ConstantComparison *eq = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::EQ, width, 0, constant_char);
    rsql::ConstantComparison *geq = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::GEQ, width, 0, constant_char);
    rsql::ConstantComparison *gt = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::GT, width, 0, constant_char);
    size_t lt_correct = 0, leq_correct = 0, eq_correct = 0, geq_correct = 0, gt_correct = 0;
    for (char *row : sample_data)
    {
        if (lt->compare(row))
        {
            lt_correct++;
        }
        if (leq->compare(row))
        {
            leq_correct++;
        }
        if (eq->compare(row))
        {
            eq_correct++;
        }
        if (geq->compare(row))
        {
            geq_correct++;
        }
        if (gt->compare(row))
        {
            gt_correct++;
        }
    }
    BOOST_CHECK(lt_correct == 4);
    BOOST_CHECK(leq_correct == 5);
    BOOST_CHECK(eq_correct == 1);
    BOOST_CHECK(geq_correct == 9);
    BOOST_CHECK(gt_correct == 8);
    delete lt;
    delete leq;
    delete eq;
    delete geq;
    delete gt;
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