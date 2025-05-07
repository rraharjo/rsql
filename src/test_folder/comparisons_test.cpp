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

BOOST_AUTO_TEST_CASE(columns_comparison_test_default_key)
{
    // def_key 32 bytes * 2
    std::vector<char *> sample_data;
    size_t width = DEFAULT_KEY_WIDTH;
    char key_1[width], key_2[width];
    std::memset(key_1, 0, width);
    std::memset(key_2, 0, width);
    key_2[width - 1] = 1;
    for (int i = 0; i < 5; i++) // 0 to 4 & 1 to 5
    {
        char *row = new char[2 * width];
        std::memcpy(row, key_1, width);
        std::memcpy(row + width, key_2, width);
        rsql::increment_default_key((unsigned char *)key_1);
        rsql::increment_default_key((unsigned char *)key_2);
        sample_data.push_back(row);
    }
    for (int i = 0; i < 5; i++) // 5 to 9 & 6
    {
        char *row = new char[2 * width];
        std::memcpy(row, key_1, width);
        std::memcpy(row + width, key_2, width);
        rsql::increment_default_key((unsigned char *)key_1);
        sample_data.push_back(row);
    }
    rsql::ColumnComparison *lt = new rsql::ColumnComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::LT, width, 0, width, width);
    rsql::ColumnComparison *leq = new rsql::ColumnComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::LEQ, width, 0, width, width);
    rsql::ColumnComparison *eq = new rsql::ColumnComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::EQ, width, 0, width, width);
    rsql::ColumnComparison *geq = new rsql::ColumnComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::GEQ, width, 0, width, width);
    rsql::ColumnComparison *gt = new rsql::ColumnComparison(rsql::DataType::DEFAULT_KEY, rsql::CompSymbol::GT, width, 0, width, width);
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
    BOOST_CHECK(lt_correct == 6);
    BOOST_CHECK(leq_correct == 7);
    BOOST_CHECK(eq_correct == 1);
    BOOST_CHECK(geq_correct == 4);
    BOOST_CHECK(gt_correct == 3);
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

BOOST_AUTO_TEST_CASE(columns_comparison_test_char)
{
    // def_key 32 bytes * 2
    std::vector<char *> sample_data;
    size_t width_1 = 6, width_2 = 12, width = width_1 + width_2;
    char key_1[width_1], key_2[width_2];
    std::memcpy(key_1, "aaaaaa", width_1);
    std::memcpy(key_2, "aaaaaaaaaaaa", width_2);
    for (int i = 0; i < 2; i++)
    {
        key_2[width_2 - 1]++;
        char *row = new char[width];
        std::memcpy(row, key_1, width_1);
        std::memcpy(row + width_1, key_2, width_2);
        sample_data.push_back(row);
    }
    key_1[width_1 - 1] = 'b';
    for (int i = 0; i < 5; i++)
    {
        key_2[width_2 - 1]++;
        char *row = new char[width];
        std::memcpy(row, key_1, width_1);
        std::memcpy(row + width_1, key_2, width_2);
        sample_data.push_back(row);
    }
    rsql::ColumnComparison *lt = new rsql::ColumnComparison(rsql::DataType::CHAR, rsql::CompSymbol::LT, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *leq = new rsql::ColumnComparison(rsql::DataType::CHAR, rsql::CompSymbol::LEQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *eq = new rsql::ColumnComparison(rsql::DataType::CHAR, rsql::CompSymbol::EQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *geq = new rsql::ColumnComparison(rsql::DataType::CHAR, rsql::CompSymbol::GEQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *gt = new rsql::ColumnComparison(rsql::DataType::CHAR, rsql::CompSymbol::GT, width_1, 0, width_2, width_1);
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
    BOOST_CHECK(lt_correct == 2);
    BOOST_CHECK(leq_correct == 2);
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

BOOST_AUTO_TEST_CASE(columns_comparison_test_date)
{
    // date (10 bytes * 2)
    std::vector<char *> sample_data;
    size_t width = DATE_TYPE_WIDTH;
    char key_1[width], key_2[width];
    std::memcpy(key_1, "2002-01-01", DATE_TYPE_WIDTH);
    std::memcpy(key_2, "2002-02-27", DATE_TYPE_WIDTH);
    for (int i = 0; i < 3; i++)
    {
        char *row = new char[2 * width];
        std::memcpy(row, key_1, width);
        std::memcpy(row + width, key_2, width);
        key_1[6]++;
        key_2[6]++;
        sample_data.push_back(row);
    }
    for (int i = 0; i < 4; i++)
    {
        char *row = new char[2 * width];
        std::memcpy(row, key_1, width);
        std::memcpy(row + width, key_2, width);
        key_1[6]++;
        sample_data.push_back(row);
    }
    rsql::ColumnComparison *lt = new rsql::ColumnComparison(rsql::DataType::DATE, rsql::CompSymbol::LT, width, 0, width, width);
    rsql::ColumnComparison *leq = new rsql::ColumnComparison(rsql::DataType::DATE, rsql::CompSymbol::LEQ, width, 0, width, width);
    rsql::ColumnComparison *eq = new rsql::ColumnComparison(rsql::DataType::DATE, rsql::CompSymbol::EQ, width, 0, width, width);
    rsql::ColumnComparison *geq = new rsql::ColumnComparison(rsql::DataType::DATE, rsql::CompSymbol::GEQ, width, 0, width, width);
    rsql::ColumnComparison *gt = new rsql::ColumnComparison(rsql::DataType::DATE, rsql::CompSymbol::GT, width, 0, width, width);
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
    BOOST_CHECK(leq_correct == 5);
    BOOST_CHECK(eq_correct == 0);
    BOOST_CHECK(geq_correct == 2);
    BOOST_CHECK(gt_correct == 2);
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

BOOST_AUTO_TEST_CASE(columns_comparison_test_unsigned_int)
{
    // date (10 bytes * 2)
    std::vector<char *> sample_data;
    size_t width_1 = 4, width_2 = 8, width = width_1 + width_2;
    cpp_int c_1 = 1200, c_2 = 500;
    for (int i = 0; i < 3; i++)
    {
        // 1200 500
        // 1200 600
        // 1200 700
        char *row = new char[width];
        rsql::ucpp_int_to_char(row, width_1, c_1);
        rsql::ucpp_int_to_char(row + width_1, width_2, c_2);
        c_2 += 100;
        sample_data.push_back(row);
    }
    for (int i = 0; i < 4; i++)
    {
        // 1200 800
        // 1000 800
        // 800  800
        // 600  800
        char *row = new char[width];
        rsql::ucpp_int_to_char(row, width_1, c_1);
        rsql::ucpp_int_to_char(row + width_1, width_2, c_2);
        c_1 -= 200;
        sample_data.push_back(row);
    }
    rsql::ColumnComparison *lt = new rsql::ColumnComparison(rsql::DataType::UINT, rsql::CompSymbol::LT, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *leq = new rsql::ColumnComparison(rsql::DataType::UINT, rsql::CompSymbol::LEQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *eq = new rsql::ColumnComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *geq = new rsql::ColumnComparison(rsql::DataType::UINT, rsql::CompSymbol::GEQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *gt = new rsql::ColumnComparison(rsql::DataType::UINT, rsql::CompSymbol::GT, width_1, 0, width_2, width_1);
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
    BOOST_CHECK(lt_correct == 1);
    BOOST_CHECK(leq_correct == 2);
    BOOST_CHECK(eq_correct == 1);
    BOOST_CHECK(geq_correct == 6);
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

BOOST_AUTO_TEST_CASE(columns_comparison_test_signed_int)
{
    // date (10 bytes * 2)
    std::vector<char *> sample_data;
    size_t width_1 = 4, width_2 = 8, width = width_1 + width_2;
    cpp_int c_1 = -3000, c_2 = 500;
    for (int i = 0; i < 5; i++)
    {
        //  -3000   500
        //  -2000   500
        //  -1000   500
        //  0       500
        //  1000    500
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 += 1000;
        sample_data.push_back(row);
    }
    for (int i = 0; i < 4; i++)
    {
        //  2000    500
        //  1500    500
        //  1000    500
        //  500     500
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 -= 500;
        sample_data.push_back(row);
    }
    rsql::ColumnComparison *lt = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LT, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *leq = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LEQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *eq = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::EQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *geq = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::GEQ, width_1, 0, width_2, width_1);
    rsql::ColumnComparison *gt = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::GT, width_1, 0, width_2, width_1);
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
    BOOST_CHECK(geq_correct == 5);
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

BOOST_AUTO_TEST_CASE(and_comparisons_test){
    std::vector<char *> sample_data;
    size_t width_1 = 4, width_2 = 8, width_3 = 4, width_4 = 8, width = width_1 + width_2+ width_3 + width_4;
    cpp_int c_1 = -3000, c_2 = 500, c_3 = 1200, c_4 = 500;
    for (int i = 0; i < 5; i++)
    {
        //  c_1     c_2     c_3     c_4
        //  -3000   500     1200    500
        //  -2000   500     1200    600
        //  -1000   500     1200    700
        //  0       500     1200    800
        //  1000    500     1200    900
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 += 1000;
        rsql::ucpp_int_to_char(row + width_1 + width_2, width_3, c_3);
        rsql::ucpp_int_to_char(row + width_1 + width_2 + width_3, width_4, c_4);
        c_4 += 100;
        sample_data.push_back(row);
    }
    for (int i = 0; i < 4; i++)
    {
        //  2000    500     1200    1000
        //  1500    500     1200    800
        //  1000    500     1200    600
        //  500     500     1200    400
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 -= 500;
        rsql::ucpp_int_to_char(row + width_1 + width_2, width_3, c_3);
        rsql::ucpp_int_to_char(row + width_1 + width_2 + width_3, width_4, c_4);
        c_4 -= 200;
        sample_data.push_back(row);
    }
    cpp_int constant_val = 700;
    char constant_val_char[width_4];
    rsql::ucpp_int_to_char(constant_val_char, width_4, constant_val);
    rsql::Comparison *col_1_less_than_col_2 = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LT, width_1, 0, width_2, width_1);
    rsql::Comparison *col_4_equal_700 = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, width_4, width_1 + width_2 + width_3, constant_val_char);
    rsql::MultiComparisons *and_comparison = new rsql::ANDComparisons();
    and_comparison->add_condition(col_1_less_than_col_2);
    and_comparison->add_condition(col_4_equal_700);
    delete col_1_less_than_col_2;
    delete col_4_equal_700;
    size_t correct = 0;
    for (char *row : sample_data){
        if (and_comparison->compare(row)){
            correct++;
        }
    }
    BOOST_CHECK(correct == 1);
    delete and_comparison;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(or_comparisons_test){
    std::vector<char *> sample_data;
    size_t width_1 = 4, width_2 = 8, width_3 = 4, width_4 = 8, width = width_1 + width_2+ width_3 + width_4;
    cpp_int c_1 = -3000, c_2 = 500, c_3 = 1200, c_4 = 500;
    for (int i = 0; i < 5; i++)
    {
        //  c_1     c_2     c_3     c_4
        //  -3000   500     1200    500
        //  -2000   500     1200    600
        //  -1000   500     1200    700
        //  0       500     1200    800
        //  1000    500     1200    900
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 += 1000;
        rsql::ucpp_int_to_char(row + width_1 + width_2, width_3, c_3);
        rsql::ucpp_int_to_char(row + width_1 + width_2 + width_3, width_4, c_4);
        c_4 += 100;
        sample_data.push_back(row);
    }
    for (int i = 0; i < 4; i++)
    {
        //  2000    500     1200    1000
        //  1500    500     1200    800
        //  1000    500     1200    600
        //  500     500     1200    400
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 -= 500;
        rsql::ucpp_int_to_char(row + width_1 + width_2, width_3, c_3);
        rsql::ucpp_int_to_char(row + width_1 + width_2 + width_3, width_4, c_4);
        c_4 -= 200;
        sample_data.push_back(row);
    }
    cpp_int constant_val = 400;
    char constant_val_char[width_4];
    rsql::ucpp_int_to_char(constant_val_char, width_4, constant_val);
    rsql::Comparison *col_1_less_than_col_2 = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LT, width_1, 0, width_2, width_1);
    rsql::Comparison *col_4_equal_400 = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, width_4, width_1 + width_2 + width_3, constant_val_char);
    rsql::MultiComparisons *or_comparison = new rsql::ORComparisons();
    or_comparison->add_condition(col_1_less_than_col_2);
    or_comparison->add_condition(col_4_equal_400);
    delete col_1_less_than_col_2;
    delete col_4_equal_400;
    size_t correct = 0;
    for (char *row : sample_data){
        if (or_comparison->compare(row)){
            correct++;
        }
    }
    BOOST_CHECK(correct == 5);
    delete or_comparison;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}

BOOST_AUTO_TEST_CASE(nested_comparisons_test){
    // sint, sint, uint, uint
    std::vector<char *> sample_data;
    size_t width_1 = 4, width_2 = 8, width_3 = 4, width_4 = 8, width = width_1 + width_2+ width_3 + width_4;
    cpp_int c_1 = -3000, c_2 = 500, c_3 = 1200, c_4 = 500;
    for (int i = 0; i < 5; i++)
    {
        //  c_1     c_2     c_3     c_4
        //  -3000   500     1200    500
        //  -2000   500     1200    600
        //  -1000   500     1200    700
        //  0       500     1200    800
        //  1000    500     1200    900
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 += 1000;
        rsql::ucpp_int_to_char(row + width_1 + width_2, width_3, c_3);
        rsql::ucpp_int_to_char(row + width_1 + width_2 + width_3, width_4, c_4);
        c_4 += 100;
        sample_data.push_back(row);
    }
    for (int i = 0; i < 4; i++)
    {
        //  2000    500     1200    1000
        //  1500    500     1200    800
        //  1000    500     1200    600
        //  500     500     1200    400
        char *row = new char[width];
        rsql::scpp_int_to_char(row, width_1, c_1);
        rsql::scpp_int_to_char(row + width_1, width_2, c_2);
        c_1 -= 500;
        rsql::ucpp_int_to_char(row + width_1 + width_2, width_3, c_3);
        rsql::ucpp_int_to_char(row + width_1 + width_2 + width_3, width_4, c_4);
        c_4 -= 200;
        sample_data.push_back(row);
    }
    // ((c_1 < c_2 && c_4 = 700) || c_1 >= 1500)
    cpp_int constant_val = 700;
    char constant_val_char[width_4];
    rsql::ucpp_int_to_char(constant_val_char, width_4, constant_val);
    rsql::Comparison *col_1_less_than_col_2 = new rsql::ColumnComparison(rsql::DataType::SINT, rsql::CompSymbol::LT, width_1, 0, width_2, width_1);
    rsql::Comparison *col_4_equal_700 = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::EQ, width_4, width_1 + width_2 + width_3, constant_val_char);
    rsql::MultiComparisons *and_comparison = new rsql::ANDComparisons();
    and_comparison->add_condition(col_1_less_than_col_2);
    and_comparison->add_condition(col_4_equal_700);
    delete col_1_less_than_col_2;
    delete col_4_equal_700;
    cpp_int const_1500 = 1500;
    char constant_val_1500[width_1];
    rsql::scpp_int_to_char(constant_val_1500, width_1, const_1500);
    rsql::Comparison *col_1_geq_1500 = new rsql::ConstantComparison(rsql::DataType::SINT, rsql::CompSymbol::GEQ, width_1, 0, constant_val_1500);
    rsql::MultiComparisons *or_comparison = new rsql::ORComparisons();
    or_comparison->add_condition(col_1_geq_1500);
    or_comparison->add_condition(and_comparison);
    delete col_1_geq_1500;
    delete and_comparison;
    size_t correct = 0;
    for (char *row : sample_data){
        if (or_comparison->compare(row)){
            correct++;
        }
    }
    BOOST_CHECK(correct == 3);
    delete or_comparison;
    for (char *row : sample_data)
    {
        delete[] row;
    }
}