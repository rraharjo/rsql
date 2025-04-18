#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "tree.h"
// Tested with degree of 2
std::string clean_this_cache = "make cleanthiscache";
BOOST_AUTO_TEST_CASE(tree_write_read_disk_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<rsql::Column> columns = tree->columns;
    uint32_t root_num = tree->root_num;
    uint32_t max_node_num = tree->max_node_num;
    uint32_t max_col_id = tree->max_col_id;
    size_t t = tree->t;
    size_t width = tree->width;
    uint32_t root_node_num = tree->root->node_num;
    delete tree;
    tree = rsql::BTree::read_disk();
    BOOST_CHECK(tree->root_num == root_num);
    BOOST_CHECK(tree->max_node_num == max_node_num);
    BOOST_CHECK(tree->max_col_id = max_col_id);
    BOOST_CHECK(tree->t == t);
    BOOST_CHECK(tree->width == width);
    BOOST_CHECK(tree->root->node_num == root_node_num);
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    char *found_0 = tree->find_row("00000000000000000000000000000000");
    char *found_4 = tree->find_row("00000000000000000000000000000004");
    char *found_7 = tree->find_row("00000000000000000000000000000007");
    char expected_0[32 + 4 + 10 + 10], expected_4[32 + 4 + 10 + 10], expected_7[32 + 4 + 10 + 10];
    std::memcpy(expected_0, "00000000000000000000000000000000", 32);
    std::memcpy(expected_4, "00000000000000000000000000000004", 32);
    std::memcpy(expected_7, "00000000000000000000000000000007", 32);
    std::memcpy(expected_0 + 32, "444410101010101010101010", 24);
    std::memcpy(expected_4 + 32, "444410101010101010101010", 24);
    std::memcpy(expected_7 + 32, "444410101010101010101010", 24);
    BOOST_CHECK(found_0 != nullptr);
    BOOST_CHECK(found_4 != nullptr);
    BOOST_CHECK(found_7 != nullptr);
    BOOST_CHECK(std::strncmp(found_0, expected_0, 56) == 0);
    BOOST_CHECK(std::strncmp(found_4, expected_4, 56) == 0);
    BOOST_CHECK(std::strncmp(found_7, expected_7, 56) == 0);
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(insert_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    char *row = tree->find_row("6bytes6bytes6bytes6bytes6bytes69");
    BOOST_CHECK(strncmp(row, "6bytes6bytes6bytes6bytes6bytes69ytes6bytes6bytes6bytes2b", 56) == 0);
    delete tree;
    delete row;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_case_1_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    char *row = tree->delete_row("00000000000000000000000000000009");
    char *not_found = tree->find_row("00000000000000000000000000000009");
    BOOST_CHECK(not_found == nullptr);
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
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    char *row = tree->delete_row("00000000000000000000000000000003");
    char *not_found = tree->find_row("00000000000000000000000000000003");
    BOOST_CHECK(not_found == nullptr);
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
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    size_t compared_bytes = 56;
    char *row_found_9 = tree->find_row("6bytes6bytes6bytes6bytes6bytes69");
    char *del_row_9 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes69");
    char *del_row_8 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes68");
    char *del_row_1 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes61");
    char *del_row_3 = tree->delete_row("6bytes6bytes6bytes6bytes6bytes63");
    char *row_nfound_9 = tree->find_row("6bytes6bytes6bytes6bytes6bytes69");
    char *row_nfound_8 = tree->find_row("6bytes6bytes6bytes6bytes6bytes68");
    char *row_nfound_1 = tree->find_row("6bytes6bytes6bytes6bytes6bytes61");
    char *row_nfound_3 = tree->find_row("6bytes6bytes6bytes6bytes6bytes63");
    char *row_found_5 = tree->find_row("6bytes6bytes6bytes6bytes6bytes65");
    char *row_found_6 = tree->find_row("6bytes6bytes6bytes6bytes6bytes66");
    BOOST_CHECK(row_nfound_9 == nullptr);
    BOOST_CHECK(row_nfound_8 == nullptr);
    BOOST_CHECK(row_nfound_1 == nullptr);
    BOOST_CHECK(row_nfound_3 == nullptr);
    BOOST_CHECK(row_found_9 != nullptr);
    BOOST_CHECK(row_found_5 != nullptr);
    BOOST_CHECK(row_found_6 != nullptr);
    BOOST_CHECK(strncmp(row_found_9, "6bytes6bytes6bytes6bytes6bytes69ytes6bytes6bytes6bytes2b", compared_bytes) == 0);
    BOOST_CHECK(strncmp(row_found_5, "6bytes6bytes6bytes6bytes6bytes65ytes6bytes6bytes6bytes2b", compared_bytes) == 0);
    BOOST_CHECK(strncmp(row_found_6, "6bytes6bytes6bytes6bytes6bytes66ytes6bytes6bytes6bytes2b", compared_bytes) == 0);
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
    delete[] row_found_5;
    delete[] row_found_6;
    delete[] row_found_9;
    delete[] del_row_9;
    delete[] del_row_8;
    delete[] del_row_1;
    delete[] del_row_3;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(add_column_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    tree->add_column(rsql::Column::char_column(0, 5));
    char *found_0 = tree->find_row("00000000000000000000000000000000");
    char *found_4 = tree->find_row("00000000000000000000000000000004");
    char *found_7 = tree->find_row("00000000000000000000000000000007");
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
    BOOST_CHECK(found_0 != nullptr);
    BOOST_CHECK(found_4 != nullptr);
    BOOST_CHECK(found_7 != nullptr);
    if (found_0)
    {
        BOOST_CHECK(std::strncmp(found_0, expected_0, 61) == 0);
    }
    if (found_4)
    {
        BOOST_CHECK(std::strncmp(found_4, expected_4, 61) == 0);
    }
    if (found_7)
    {
        BOOST_CHECK(std::strncmp(found_7, expected_7, 61) == 0);
    }
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(remove_column_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    tree->remove_column(1);
    tree->remove_column(1);
    char *found_0 = tree->find_row("00000000000000000000000000000000");
    char *found_4 = tree->find_row("00000000000000000000000000000004");
    char *found_7 = tree->find_row("00000000000000000000000000000007");
    char expected_0[32 + 10], expected_4[32 + 10], expected_7[32 + 10];
    std::memcpy(expected_0, "00000000000000000000000000000000", 32);
    std::memcpy(expected_4, "00000000000000000000000000000004", 32);
    std::memcpy(expected_7, "00000000000000000000000000000007", 32);
    std::memcpy(expected_0 + 32, "1010101010", 10);
    std::memcpy(expected_4 + 32, "1010101010", 10);
    std::memcpy(expected_7 + 32, "1010101010", 10);
    BOOST_CHECK(found_0 != nullptr);
    BOOST_CHECK(found_4 != nullptr);
    BOOST_CHECK(found_7 != nullptr);
    if (found_0)
    {
        BOOST_CHECK(std::strncmp(found_0, expected_0, 42) == 0);
    }
    if (found_4)
    {
        BOOST_CHECK(std::strncmp(found_4, expected_4, 42) == 0);
    }
    if (found_7)
    {
        BOOST_CHECK(std::strncmp(found_7, expected_7, 42) == 0);
    }
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(modify_column_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    tree->remove_column(1);
    tree->add_column(rsql::Column::date_column(0));
    tree->remove_column(1);
    tree->add_column(rsql::Column::char_column(0, 10));
    char *found_0 = tree->find_row("00000000000000000000000000000000");
    char *found_4 = tree->find_row("00000000000000000000000000000004");
    char *found_7 = tree->find_row("00000000000000000000000000000007");
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
    BOOST_CHECK(found_0 != nullptr);
    BOOST_CHECK(found_4 != nullptr);
    BOOST_CHECK(found_7 != nullptr);
    if (found_0)
    {
        BOOST_CHECK(std::strncmp(found_0, expected_0, 62) == 0);
    }
    if (found_4)
    {
        BOOST_CHECK(std::strncmp(found_4, expected_4, 62) == 0);
    }
    if (found_7)
    {
        BOOST_CHECK(std::strncmp(found_7, expected_7, 62) == 0);
    }
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_all_indexed_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> expected;
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    src[PKEY_COL_W - 1] = '5';
    char *expected_item = new char[32 + 4 + 10 + 10];
    std::memcpy(expected_item, src, 32 + 4 + 10 + 10);
    expected.push_back(expected_item);
    for (int i = 0; i < 3; i++)
    {
        src[PKEY_COL_W]++;
        tree->insert_row(src);
        char *expected_i = new char[32 + 4 + 10 + 10];
        std::memcpy(expected_i, src, 32 + 4 + 10 + 10);
        expected.push_back(expected_i);
    }
    char key[PKEY_COL_W];
    std::memcpy(key, "00000000000000000000000000000005", PKEY_COL_W);
    std::vector<char *> alls = tree->find_all_row(key, 0);
    BOOST_CHECK(alls.size() == 4);
    BOOST_CHECK(expected.size() == alls.size());
    for (int i = 0; i < alls.size(); i++)
    {
        bool found = false;
        for (int j = 0; j < expected.size(); j++)
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
    {
        delete[] single;
    }
    for (char *item : expected)
    {
        delete[] item;
    }
    delete tree;
    std::system(clean_this_cache.c_str());
}
BOOST_AUTO_TEST_CASE(find_all_unindexed)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> expected;
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 7; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    src[34] = '2';
    for (int i = 0; i < 3; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1];
    }
    std::vector<char *> found = tree->find_all_row("4424", 1);
    BOOST_CHECK(found.size() == 3);
    for (int i = 0; i < found.size(); i++)
    {
        bool correct_id = found[i][PKEY_COL_W - 1] == '7' || found[i][PKEY_COL_W - 1] == '8' || found[i][PKEY_COL_W - 1] == '9';
        BOOST_CHECK(correct_id);
        BOOST_CHECK(strncmp(found[i] + PKEY_COL_W, "4424", 4) == 0);
    }
    for (int i = 0; i < found.size(); i++)
    {
        delete[] found[i];
    }
    delete tree;
    std::system(clean_this_cache.c_str());
}
BOOST_AUTO_TEST_CASE(batch_delete_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> deleted;
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        if (i < 4)
        {
            char *del_key = new char[32 + 4 + 10 + 10];
            std::memcpy(del_key, src, 32 + 4 + 10 + 10);
            deleted.push_back(del_key);
        }
        src[PKEY_COL_W - 1]++;
    }
    std::vector<char *> rows = tree->batch_delete(deleted);
    for (const char *item : deleted)
    {
        const char *res = tree->find_row(item);
        BOOST_CHECK(res == nullptr);
        delete[] res;
    }
    for (const char *row : rows)
    {
        delete[] row;
    }
    for (const char *item : deleted)
    {
        delete[] item;
    }
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_all_indexed_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char key_3[PKEY_COL_W], key_5[PKEY_COL_W];
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 10; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    src[PKEY_COL_W - 1] = '5';
    std::memcpy(key_5, src, PKEY_COL_W);
    for (int i = 0; i < 3; i++)
    {
        tree->insert_row(src);
    }
    src[PKEY_COL_W - 1] = '3';
    std::memcpy(key_3, src, PKEY_COL_W);
    for (int i = 0; i < 2; i++)
    {
        tree->insert_row(src);
    }

    std::vector<char *> deleted_3 = tree->delete_all(key_3, 0);
    std::vector<char *> deleted_5 = tree->delete_all(key_5, 0);

    src[PKEY_COL_W - 1] = '3';
    BOOST_CHECK(deleted_3.size() == 3);
    for (int i = 0; i < deleted_3.size(); i++)
    {
        BOOST_CHECK(strncmp(deleted_3[i], src, 32 + 4 + 10 + 10) == 0);
    }

    src[PKEY_COL_W - 1] = '5';
    BOOST_CHECK(deleted_5.size() == 4);
    for (int i = 0; i < deleted_5.size(); i++)
    {
        BOOST_CHECK(strncmp(deleted_5[i], src, 32 + 4 + 10 + 10) == 0);
    }

    for (int i = 0; i < deleted_3.size(); i++)
    {
        delete[] deleted_3[i];
    }
    for (int i = 0; i < deleted_5.size(); i++)
    {
        delete[] deleted_5[i];
    }
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(delete_all_unindexed_test)
{
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char key_1[4], key_9[4];
    std::memcpy(key_1, "4414", 4);
    std::memcpy(key_9, "4494", 4);
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0; i < 5; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }

    src[34] = '1';
    for (int i = 0; i < 3; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    src[34] = '9';
    for (int i = 0; i < 2; i++)
    {
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }

    std::vector<char *> deleted_1 = tree->delete_all(key_1, 1);
    std::vector<char *> deleted_9 = tree->delete_all(key_9, 1);

    BOOST_CHECK(deleted_1.size() == 3);
    for (int i = 0; i < deleted_1.size(); i++)
    {
        bool correct_id = deleted_1[i][PKEY_COL_W - 1] == '5' || deleted_1[i][PKEY_COL_W - 1] == '6' || deleted_1[i][PKEY_COL_W - 1] == '7';
        BOOST_CHECK(deleted_1[i][34] == '1');
        BOOST_CHECK(correct_id);
    }

    BOOST_CHECK(deleted_9.size() == 2);
    for (int i = 0; i < deleted_9.size(); i++)
    {
        bool correct_id = deleted_9[i][PKEY_COL_W - 1] == '8' || deleted_9[i][PKEY_COL_W - 1] == '9';
        BOOST_CHECK(deleted_9[i][34] == '9');
        BOOST_CHECK(correct_id);
    }

    std::memcpy(src, "00000000000000000000000000000000444410101010101010101010", 56);
    for (int i = 0; i < 5; i++)
    {
        char *not_deleted = tree->find_row(src);
        BOOST_CHECK(not_deleted != nullptr);
        if (not_deleted)
        {
            BOOST_CHECK(strncmp(not_deleted, src, 56) == 0);
        }
        delete[] not_deleted;
        src[PKEY_COL_W - 1]++;
    }

    for (int i = 0; i < deleted_1.size(); i++)
    {
        delete[] deleted_1[i];
    }
    for (int i = 0; i < deleted_9.size(); i++)
    {
        delete[] deleted_9[i];
    }
    delete tree;
    std::system(clean_this_cache.c_str());
}