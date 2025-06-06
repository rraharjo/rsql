#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "tree.h"
typedef boost::multiprecision::cpp_int cpp_int;
std::string clean_this_cache = "make cleancache";
BOOST_AUTO_TEST_CASE(tree_write_read_disk_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    uint32_t root_num = tree->get_root_num();
    uint32_t max_node_num = tree->get_max_node_num();
    uint32_t max_col_id = tree->get_max_col_id();
    size_t t = tree->get_degree();
    size_t width = tree->get_width();
    delete tree;
    tree = rsql::BTree::read_disk();
    BOOST_CHECK(tree->get_root_num() == root_num);
    BOOST_CHECK(tree->get_max_node_num() == max_node_num);
    BOOST_CHECK(tree->get_max_col_id() == max_col_id);
    BOOST_CHECK(tree->get_degree() == t);
    BOOST_CHECK(tree->get_width() == width);
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    std::vector<char *> found_0 = tree->find_all_row("00000000000000000000000000000000", 0);
    std::vector<char *> found_4 = tree->find_all_row("00000000000000000000000000000004", 0);
    std::vector<char *> found_7 = tree->find_all_row("00000000000000000000000000000007", 0);
    char expected_0[32 + 4 + 10 + 10], expected_4[32 + 4 + 10 + 10], expected_7[32 + 4 + 10 + 10];
    std::memcpy(expected_0, "00000000000000000000000000000000", 32);
    std::memcpy(expected_4, "00000000000000000000000000000004", 32);
    std::memcpy(expected_7, "00000000000000000000000000000007", 32);
    std::memcpy(expected_0 + 32, "444410101010101010101010", 24);
    std::memcpy(expected_4 + 32, "444410101010101010101010", 24);
    std::memcpy(expected_7 + 32, "444410101010101010101010", 24);
    BOOST_CHECK(found_0.size() == 1);
    BOOST_CHECK(found_4.size() == 1);
    BOOST_CHECK(found_7.size() == 1);
    BOOST_CHECK(found_0[0] != nullptr);
    BOOST_CHECK(found_4[0] != nullptr);
    BOOST_CHECK(found_7[0] != nullptr);
    BOOST_CHECK(std::strncmp(found_0[0], expected_0, 56) == 0);
    BOOST_CHECK(std::strncmp(found_4[0], expected_4, 56) == 0);
    BOOST_CHECK(std::strncmp(found_7[0], expected_7, 56) == 0);
    delete found_0[0];
    delete found_4[0];
    delete found_7[0];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(insert_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    std::vector<char *> row = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes69", 0);
    BOOST_CHECK(row.size() == 1);
    BOOST_CHECK(row[0] != nullptr);
    BOOST_CHECK(strncmp(row[0], "6bytes6bytes6bytes6bytes6bytes69ytes6bytes6bytes6bytes2b", 56) == 0);
    delete tree;
    delete row[0];
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_case_1_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    char *row = tree->delete_row("00000000000000000000000000000009");
    std::vector<char *> not_found = tree->find_all_row("00000000000000000000000000000009", 0);
    BOOST_CHECK(not_found.size() == 0);
    BOOST_CHECK(row != nullptr);
    if (row)
    {
        BOOST_CHECK(strncmp(row, "00000000000000000000000000000009444410101010101010101010", 56) == 0);
    }
    delete[] row;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_case_2_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    char *row = tree->delete_row("00000000000000000000000000000003");
    std::vector<char *> not_found = tree->find_all_row("00000000000000000000000000000003", 0);
    BOOST_CHECK(not_found.size() == 0);
    BOOST_CHECK(row != nullptr);
    if (row)
    {
        BOOST_CHECK(strncmp(row, "00000000000000000000000000000003444410101010101010101010", 56) == 0);
    }
    delete[] row;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    size_t compared_bytes = 56;
    std::vector<char *> row_found_9 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes69", 0);
    char *del_row_9 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes69");
    char *del_row_8 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes68");
    char *del_row_1 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes61");
    char *del_row_3 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes63");
    std::vector<char *> row_nfound_9 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes69", 0);
    std::vector<char *> row_nfound_8 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes68", 0);
    std::vector<char *> row_nfound_1 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes61", 0);
    std::vector<char *> row_nfound_3 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes63", 0);
    std::vector<char *> row_found_5 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes65", 0);
    std::vector<char *> row_found_6 = tree->find_all_row("6bytes6bytes6bytes6bytes6bytes66", 0);
    BOOST_CHECK(row_nfound_9.size() == 0);
    BOOST_CHECK(row_nfound_8.size() == 0);
    BOOST_CHECK(row_nfound_1.size() == 0);
    BOOST_CHECK(row_nfound_3.size() == 0);
    BOOST_CHECK(row_found_9.size() == 1);
    BOOST_CHECK(row_found_5.size() == 1);
    BOOST_CHECK(row_found_6.size() == 1);
    BOOST_CHECK(row_found_9[0] != nullptr);
    BOOST_CHECK(row_found_5[0] != nullptr);
    BOOST_CHECK(row_found_6[0] != nullptr);
    BOOST_CHECK(strncmp(row_found_9[0], "6bytes6bytes6bytes6bytes6bytes69ytes6bytes6bytes6bytes2b", compared_bytes) == 0);
    BOOST_CHECK(strncmp(row_found_5[0], "6bytes6bytes6bytes6bytes6bytes65ytes6bytes6bytes6bytes2b", compared_bytes) == 0);
    BOOST_CHECK(strncmp(row_found_6[0], "6bytes6bytes6bytes6bytes6bytes66ytes6bytes6bytes6bytes2b", compared_bytes) == 0);
    BOOST_CHECK(del_row_9 != nullptr);
    BOOST_CHECK(del_row_8 != nullptr);
    BOOST_CHECK(del_row_1 != nullptr);
    BOOST_CHECK(del_row_3 != nullptr);
    if (del_row_9)
    {
        BOOST_CHECK(strncmp(del_row_9, "6bytes6bytes6bytes6bytes6bytes69ytes6bytes6bytes6bytes2b", 56) == 0);
    }
    if (del_row_8)
    {
        BOOST_CHECK(strncmp(del_row_8, "6bytes6bytes6bytes6bytes6bytes68ytes6bytes6bytes6bytes2b", 56) == 0);
    }
    if (del_row_1)
    {
        BOOST_CHECK(strncmp(del_row_1, "6bytes6bytes6bytes6bytes6bytes61ytes6bytes6bytes6bytes2b", 56) == 0);
    }
    if (del_row_3)
    {
        BOOST_CHECK(strncmp(del_row_3, "6bytes6bytes6bytes6bytes6bytes63ytes6bytes6bytes6bytes2b", 56) == 0);
    }
    delete tree;
    delete[] row_found_5[0];
    delete[] row_found_6[0];
    delete[] row_found_9[0];
    delete[] del_row_9;
    delete[] del_row_8;
    delete[] del_row_1;
    delete[] del_row_3;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(add_column_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    tree->add_column(rsql::Column::char_column(0, 5));
    std::vector<char *> found_0 = tree->find_all_row("00000000000000000000000000000000", 0);
    std::vector<char *> found_4 = tree->find_all_row("00000000000000000000000000000004", 0);
    std::vector<char *> found_7 = tree->find_all_row("00000000000000000000000000000007", 0);
    char expected_0[32 + 4 + 10 + 10 + 5], expected_4[32 + 4 + 10 + 10 + 5], expected_7[32 + 4 + 10 + 10 + 5];
    std::memcpy(expected_0, "00000000000000000000000000000000", 32);
    std::memcpy(expected_4, "00000000000000000000000000000004", 32);
    std::memcpy(expected_7, "00000000000000000000000000000007", 32);
    std::memcpy(expected_0 + 32, "444410101010101010101010", 24);
    std::memcpy(expected_4 + 32, "444410101010101010101010", 24);
    std::memcpy(expected_7 + 32, "444410101010101010101010", 24);
    std::memset(expected_0 + 56, 0, 5);
    std::memset(expected_4 + 56, 0, 5);
    std::memset(expected_7 + 56, 0, 5);
    BOOST_CHECK(found_0.size() == 1);
    BOOST_CHECK(found_4.size() == 1);
    BOOST_CHECK(found_7.size() == 1);
    BOOST_CHECK(found_0[0] != nullptr);
    BOOST_CHECK(found_4[0] != nullptr);
    BOOST_CHECK(found_7[0] != nullptr);
    BOOST_CHECK(std::strncmp(found_0[0], expected_0, 61) == 0);
    BOOST_CHECK(std::strncmp(found_4[0], expected_4, 61) == 0);
    BOOST_CHECK(std::strncmp(found_7[0], expected_7, 61) == 0);
    delete found_0[0];
    delete found_4[0];
    delete found_7[0];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(remove_column_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    tree->remove_column(1);
    tree->remove_column(1);
    std::vector<char *> found_0 = tree->find_all_row("00000000000000000000000000000000", 0);
    std::vector<char *> found_4 = tree->find_all_row("00000000000000000000000000000004", 0);
    std::vector<char *> found_7 = tree->find_all_row("00000000000000000000000000000007", 0);
    char expected_0[32 + 10], expected_4[32 + 10], expected_7[32 + 10];
    std::memcpy(expected_0, "00000000000000000000000000000000", 32);
    std::memcpy(expected_4, "00000000000000000000000000000004", 32);
    std::memcpy(expected_7, "00000000000000000000000000000007", 32);
    std::memcpy(expected_0 + 32, "1010101010", 10);
    std::memcpy(expected_4 + 32, "1010101010", 10);
    std::memcpy(expected_7 + 32, "1010101010", 10);
    BOOST_CHECK(found_0.size() == 1);
    BOOST_CHECK(found_4.size() == 1);
    BOOST_CHECK(found_7.size() == 1);
    BOOST_CHECK(found_0[0] != nullptr);
    BOOST_CHECK(found_4[0] != nullptr);
    BOOST_CHECK(found_7[0] != nullptr);

    BOOST_CHECK(std::strncmp(found_0[0], expected_0, 42) == 0);
    BOOST_CHECK(std::strncmp(found_4[0], expected_4, 42) == 0);
    BOOST_CHECK(std::strncmp(found_7[0], expected_7, 42) == 0);
    delete found_0[0];
    delete found_4[0];
    delete found_7[0];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(modify_column_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    tree->remove_column(1);
    tree->add_column(rsql::Column::date_column(0));
    tree->remove_column(1);
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> found_0 = tree->find_all_row("00000000000000000000000000000000", 0);
    std::vector<char *> found_4 = tree->find_all_row("00000000000000000000000000000004", 0);
    std::vector<char *> found_7 = tree->find_all_row("00000000000000000000000000000007", 0);
    char expected_0[32 + 10 + 10 + 10], expected_4[32 + 10 + 10 + 10], expected_7[32 + 10 + 10 + 10];
    std::memcpy(expected_0, "00000000000000000000000000000000", 32);
    std::memcpy(expected_4, "00000000000000000000000000000004", 32);
    std::memcpy(expected_7, "00000000000000000000000000000007", 32);
    std::memcpy(expected_0 + 32, "1010101010", 10);
    std::memcpy(expected_4 + 32, "1010101010", 10);
    std::memcpy(expected_7 + 32, "1010101010", 10);
    std::memset(expected_0 + 42, 0, 20);
    std::memset(expected_4 + 42, 0, 20);
    std::memset(expected_7 + 42, 0, 20);
    BOOST_CHECK(found_0.size() == 1);
    BOOST_CHECK(found_4.size() == 1);
    BOOST_CHECK(found_7.size() == 1);
    BOOST_CHECK(found_0[0] != nullptr);
    BOOST_CHECK(found_4[0] != nullptr);
    BOOST_CHECK(found_7[0] != nullptr);
    BOOST_CHECK(std::strncmp(found_0[0], expected_0, 62) == 0);
    BOOST_CHECK(std::strncmp(found_4[0], expected_4, 62) == 0);
    BOOST_CHECK(std::strncmp(found_7[0], expected_7, 62) == 0);
    delete found_0[0];
    delete found_4[0];
    delete found_7[0];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_all_indexed_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree(nullptr, 0, false);
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> expected;
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    src[DEFAULT_KEY_WIDTH - 1] = '5';
    char *expected_item = new char[32 + 4 + 10 + 10];
    std::memcpy(expected_item, src, 32 + 4 + 10 + 10);
    expected.push_back(expected_item);
    for (int i = 0; i < 3; i++)
    {
        src[DEFAULT_KEY_WIDTH]++;
        tree->insert_row(src);
        char *expected_i = new char[32 + 4 + 10 + 10];
        std::memcpy(expected_i, src, 32 + 4 + 10 + 10);
        expected.push_back(expected_i);
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memcpy(key, "00000000000000000000000000000005", DEFAULT_KEY_WIDTH);
    std::vector<char *> alls = tree->find_all_row(key, 0);
    BOOST_CHECK(alls.size() == 4);
    BOOST_CHECK(expected.size() == alls.size());
    for (size_t i = 0; i < alls.size(); i++)
    {
        bool found = false;
        for (size_t j = 0; j < expected.size(); j++)
        {
            if (strncmp(alls[i], alls[j], 32 + 4 + 10 + 10) == 0)
            {
                found = true;
                break;
            }
        }
        BOOST_CHECK(found);
    }
    for (char *single : alls)
        delete[] single;
    for (char *item : expected)
        delete[] item;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_all_unindexed)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> expected;
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 7; i++)
    {
        tree->insert_row(src);
        src[DEFAULT_KEY_WIDTH - 1]++;
    }
    src[34] = '2';
    for (int i = 0; i < 3; i++)
    {
        tree->insert_row(src);
    }
    std::vector<char *> found = tree->find_all_row("4424", 1);
    BOOST_CHECK(found.size() == 3);
    for (size_t i = 0; i < found.size(); i++)
    {
        bool correct_id = found[i][DEFAULT_KEY_WIDTH - 1] == '7' || found[i][DEFAULT_KEY_WIDTH - 1] == '8' || found[i][DEFAULT_KEY_WIDTH - 1] == '9';
        BOOST_CHECK(correct_id);
        BOOST_CHECK(strncmp(found[i] + DEFAULT_KEY_WIDTH, "4424", 4) == 0);
    }
    for (size_t i = 0; i < found.size(); i++)
        delete[] found[i];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(indexed_search_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));

    char row[DEFAULT_KEY_WIDTH + 4 + 10 + 10];
    std::memset(row, 0, DEFAULT_KEY_WIDTH);
    cpp_int num = 5000;
    rsql::ucpp_int_to_char(row + DEFAULT_KEY_WIDTH, 4, num);
    std::memcpy(row + DEFAULT_KEY_WIDTH + 4, "2002-01-10", 10);
    std::memset(row + DEFAULT_KEY_WIDTH + 14, 'A', 10);
    for (int i = 0; i < 50; i++)
    {
        tree->insert_row(row);
        rsql::increment_default_key((unsigned char *)row);
    }
    char search_key[DEFAULT_KEY_WIDTH];
    std::memset(search_key, 0, DEFAULT_KEY_WIDTH);
    search_key[DEFAULT_KEY_WIDTH - 1] = 10;
    std::vector<char *> results = tree->search_rows(search_key, rsql::CompSymbol::LT);
    BOOST_CHECK(results.size() == 10);
    for (const char *result : results)
    {
        BOOST_CHECK(result[DEFAULT_KEY_WIDTH - 1] < 10);
        BOOST_CHECK(result[DEFAULT_KEY_WIDTH - 1] >= 0);
        BOOST_CHECK(std::memcmp(result + DEFAULT_KEY_WIDTH, row + DEFAULT_KEY_WIDTH, 24) == 0);
    }
    for (const char *result : results)
        delete[] result;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(indexed_search_with_comparison_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));

    char row[DEFAULT_KEY_WIDTH + 4 + 10 + 10];
    std::memset(row, 0, DEFAULT_KEY_WIDTH);
    cpp_int num = 5000;
    rsql::ucpp_int_to_char(row + DEFAULT_KEY_WIDTH, 4, num);
    std::memcpy(row + DEFAULT_KEY_WIDTH + 4, "2002-01-10", 10);
    std::memset(row + DEFAULT_KEY_WIDTH + 14, 'A', 10);
    for (int i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        rsql::increment_default_key((unsigned char *)row);
    }
    std::memcpy(row + DEFAULT_KEY_WIDTH + 4, "2002-12-12", 10);
    for (int i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        rsql::increment_default_key((unsigned char *)row);
    }
    char search_key[DEFAULT_KEY_WIDTH];
    std::memset(search_key, 0, DEFAULT_KEY_WIDTH);
    search_key[DEFAULT_KEY_WIDTH - 1] = 30;
    rsql::Comparison *eq_date = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::EQ, 10, 36, "2002-12-12");
    std::vector<char *> results = tree->search_rows(search_key, rsql::CompSymbol::LT, eq_date);
    BOOST_CHECK(results.size() == 5);
    for (const char *result : results)
    {
        BOOST_CHECK(result[DEFAULT_KEY_WIDTH - 1] < 30);
        BOOST_CHECK(result[DEFAULT_KEY_WIDTH - 1] >= 25);
        BOOST_CHECK(std::memcmp(result + DEFAULT_KEY_WIDTH, row + DEFAULT_KEY_WIDTH, 24) == 0);
    }
    delete eq_date;
    for (const char *result : results)
        delete[] result;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(linear_search_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));

    char row[DEFAULT_KEY_WIDTH + 4 + 10 + 10];
    std::memset(row, 0, DEFAULT_KEY_WIDTH);
    cpp_int num = 5000;
    rsql::ucpp_int_to_char(row + DEFAULT_KEY_WIDTH, 4, num);
    std::memcpy(row + DEFAULT_KEY_WIDTH + 4, "2002-01-10", 10);
    std::memset(row + DEFAULT_KEY_WIDTH + 14, 'A', 10);
    for (int i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        rsql::increment_default_key((unsigned char *)row);
    }
    std::memcpy(row + DEFAULT_KEY_WIDTH + 4, "2002-12-12", 10);
    for (int i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        rsql::increment_default_key((unsigned char *)row);
    }
    rsql::Comparison *eq_date = new rsql::ConstantComparison(rsql::DataType::DATE, rsql::CompSymbol::EQ, 10, 36, "2002-12-12");
    std::vector<char *> results = tree->search_rows(nullptr, rsql::CompSymbol::LT, eq_date);
    BOOST_CHECK(results.size() == 25);
    for (const char *result : results)
    {
        BOOST_CHECK(result[DEFAULT_KEY_WIDTH - 1] < 50);
        BOOST_CHECK(result[DEFAULT_KEY_WIDTH - 1] >= 25);
        BOOST_CHECK(std::memcmp(result + DEFAULT_KEY_WIDTH, row + DEFAULT_KEY_WIDTH, 24) == 0);
    }
    delete eq_date;
    for (const char *result : results)
        delete[] result;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_row_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree(nullptr, 0, false);
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    size_t row_width = DEFAULT_KEY_WIDTH + 4 + 10 + 10;
    char row[row_width];
    std::memset(row, 0, row_width);
    cpp_int ucpp = 0;
    std::memcpy(row + 36, "2025/05/14", 10);
    std::memcpy(row + 46, "SUPERBAD", 8);
    for (size_t i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    rsql::increment_default_key(reinterpret_cast<unsigned char *>(row));
    for (size_t i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    std::vector<char *> res = tree->delete_all_row(key);
    std::vector<char *> not_found = tree->search_rows(key);
    BOOST_CHECK(res.size() == 25);
    for (size_t i = 0; i < res.size(); i++)
    {
        BOOST_CHECK(std::memcmp(res[i], key, DEFAULT_KEY_WIDTH) == 0);
        cpp_int cur_ucpp;
        rsql::char_to_ucpp_int(res[i] + DEFAULT_KEY_WIDTH, 4, cur_ucpp);
        BOOST_CHECK(cur_ucpp >= 0);
        BOOST_CHECK(cur_ucpp < 25);
        BOOST_CHECK(std::strncmp(res[i] + 36, "2025/05/14", 10) == 0);
        BOOST_CHECK(std::strncmp(res[i] + 46, "SUPERBAD", 8) == 0);
    }
    BOOST_CHECK(not_found.size() == 0);
    for (size_t i = 0; i < res.size(); i++)
        delete[] res[i];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_row_with_comparator_test)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree(nullptr, 0, false);
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    size_t row_width = DEFAULT_KEY_WIDTH + 4 + 10 + 10;
    char row[row_width];
    std::memset(row, 0, row_width);
    cpp_int ucpp = 0;
    std::memcpy(row + 36, "2025/05/14", 10);
    std::memcpy(row + 46, "SUPERBAD", 8);
    for (size_t i = 0; i < 50; i++)
    {
        tree->insert_row(row);
        rsql::increment_default_key(reinterpret_cast<unsigned char *>(row));
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    key[DEFAULT_KEY_WIDTH - 1] = 23;
    std::vector<char *> res = tree->delete_all_row(key, rsql::CompSymbol::LEQ);
    std::vector<char *> not_found = tree->search_rows(key);
    BOOST_CHECK(res.size() == 24);
    for (size_t i = 0; i < res.size(); i++)
    {
        BOOST_CHECK(res[i][DEFAULT_KEY_WIDTH - 1] >= 0);
        BOOST_CHECK(res[i][DEFAULT_KEY_WIDTH - 1] <= 23);
        cpp_int cur_ucpp;
        rsql::char_to_ucpp_int(res[i] + DEFAULT_KEY_WIDTH, 4, cur_ucpp);
        BOOST_CHECK(cur_ucpp >= 0);
        BOOST_CHECK(cur_ucpp <= 23);
        BOOST_CHECK(std::strncmp(res[i] + 36, "2025/05/14", 10) == 0);
        BOOST_CHECK(std::strncmp(res[i] + 46, "SUPERBAD", 8) == 0);
    }
    BOOST_CHECK(not_found.size() == 0);
    for (size_t i = 0; i < res.size(); i++)
        delete[] res[i];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_row_test_with_comparison)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree(nullptr, 0, false);
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    size_t row_width = DEFAULT_KEY_WIDTH + 4 + 10 + 10;
    char row[row_width];
    std::memset(row, 0, row_width);
    cpp_int ucpp = 0;
    std::memcpy(row + 36, "2025/05/14", 10);
    std::memcpy(row + 46, "SUPERBAD", 8);
    for (size_t i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    rsql::increment_default_key(reinterpret_cast<unsigned char *>(row));
    for (size_t i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    cpp_int comp_val = 10;
    char comp_val_char[4];
    rsql::ucpp_int_to_char(comp_val_char, 4, comp_val);
    rsql::Comparison *ucpp_gt_10 = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::GT, 4, 32, comp_val_char);
    std::vector<char *> res = tree->delete_all_row(key, rsql::CompSymbol::EQ, ucpp_gt_10);
    std::vector<char *> found_some = tree->search_rows(key);
    BOOST_CHECK(res.size() == 14);
    for (size_t i = 0; i < res.size(); i++)
    {
        BOOST_CHECK(std::memcmp(res[i], key, DEFAULT_KEY_WIDTH) == 0);
        cpp_int cur_ucpp;
        rsql::char_to_ucpp_int(res[i] + DEFAULT_KEY_WIDTH, 4, cur_ucpp);
        BOOST_CHECK(cur_ucpp > 10);
        BOOST_CHECK(cur_ucpp < 25);
        BOOST_CHECK(std::strncmp(res[i] + 36, "2025/05/14", 10) == 0);
        BOOST_CHECK(std::strncmp(res[i] + 46, "SUPERBAD", 8) == 0);
    }
    BOOST_CHECK(found_some.size() == 11);
    for (size_t i = 0; i < found_some.size(); i++)
    {
        BOOST_CHECK(std::memcmp(found_some[i], key, DEFAULT_KEY_WIDTH) == 0);
        cpp_int cur_ucpp;
        rsql::char_to_ucpp_int(found_some[i] + DEFAULT_KEY_WIDTH, 4, cur_ucpp);
        BOOST_CHECK(cur_ucpp >= 0);
        BOOST_CHECK(cur_ucpp < 11);
        BOOST_CHECK(std::strncmp(found_some[i] + 36, "2025/05/14", 10) == 0);
        BOOST_CHECK(std::strncmp(found_some[i] + 46, "SUPERBAD", 8) == 0);
    }
    delete ucpp_gt_10;
    for (size_t i = 0; i < res.size(); i++)
        delete[] res[i];
    for (size_t i = 0; i < found_some.size(); i++)
        delete[] found_some[i];
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_row_with_linear_search)
{
    rsql::BTree *tree = rsql::BTree::create_new_tree(nullptr, 0, false);
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::unsigned_int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    size_t row_width = DEFAULT_KEY_WIDTH + 4 + 10 + 10;
    char row[row_width];
    std::memset(row, 0, row_width);
    cpp_int ucpp = 0;
    std::memcpy(row + 36, "2025/05/14", 10);
    std::memcpy(row + 46, "SUPERBAD", 8);
    for (size_t i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    rsql::increment_default_key(reinterpret_cast<unsigned char *>(row));
    for (size_t i = 0; i < 25; i++)
    {
        tree->insert_row(row);
        ucpp++;
        rsql::ucpp_int_to_char(row + 32, 4, ucpp);
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    cpp_int comp_val = 10;
    char comp_val_char[4];
    rsql::ucpp_int_to_char(comp_val_char, 4, comp_val);
    rsql::Comparison *ucpp_gt_10 = new rsql::ConstantComparison(rsql::DataType::UINT, rsql::CompSymbol::GT, 4, 32, comp_val_char);
    std::vector<char *> res = tree->delete_all_row(nullptr, rsql::CompSymbol::EQ, ucpp_gt_10);
    std::vector<char *> found_some = tree->search_rows(key);
    BOOST_CHECK(res.size() == 39);
    for (size_t i = 0; i < res.size(); i++)
    {
        BOOST_CHECK(res[i][DEFAULT_KEY_WIDTH - 1] == 0 || res[i][DEFAULT_KEY_WIDTH - 1] == 1);
        cpp_int cur_ucpp;
        rsql::char_to_ucpp_int(res[i] + DEFAULT_KEY_WIDTH, 4, cur_ucpp);
        BOOST_CHECK(cur_ucpp > 10);
        BOOST_CHECK(cur_ucpp < 50);
        BOOST_CHECK(std::strncmp(res[i] + 36, "2025/05/14", 10) == 0);
        BOOST_CHECK(std::strncmp(res[i] + 46, "SUPERBAD", 8) == 0);
    }
    BOOST_CHECK(found_some.size() == 11);
    for (size_t i = 0; i < found_some.size(); i++)
    {
        BOOST_CHECK(std::memcmp(found_some[i], key, DEFAULT_KEY_WIDTH) == 0);
        cpp_int cur_ucpp;
        rsql::char_to_ucpp_int(found_some[i] + DEFAULT_KEY_WIDTH, 4, cur_ucpp);
        BOOST_CHECK(cur_ucpp >= 0);
        BOOST_CHECK(cur_ucpp < 11);
        BOOST_CHECK(std::strncmp(found_some[i] + 36, "2025/05/14", 10) == 0);
        BOOST_CHECK(std::strncmp(found_some[i] + 46, "SUPERBAD", 8) == 0);
    }
    delete ucpp_gt_10;
    for (size_t i = 0; i < res.size(); i++)
        delete[] res[i];
    for (size_t i = 0; i < found_some.size(); i++)
        delete[] found_some[i];
    delete tree;
    std::system(clean_this_cache.c_str());
}