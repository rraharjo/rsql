#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "tree.h"
BOOST_AUTO_TEST_CASE(tree_write_read_disk_test){
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
    system("make cleandb");
}

BOOST_AUTO_TEST_CASE(find_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0 ; i < 10 ; i++){
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
    std::system("make cleandb");
}

BOOST_AUTO_TEST_CASE(insert_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0 ; i < 10 ; i++){
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    char *row = tree->find_row("6bytes6bytes6bytes6bytes6bytes69");
    BOOST_CHECK(strncmp(row, "6bytes6bytes6bytes6bytes6bytes69ytes6bytes6bytes6bytes2b", 56) == 0);
    delete tree;
    delete row;
    system("make cleandb");
}

BOOST_AUTO_TEST_CASE(delete_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "6bytes6bytes6bytes6bytes6bytes60ytes6bytes6bytes6bytes2b";
    for (int i = 0 ; i < 10 ; i++){
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    size_t compared_bytes = 56;
    char *row_found_9 = tree->find_row("6bytes6bytes6bytes6bytes6bytes69");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes69");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes68");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes61");
    tree->delete_row("6bytes6bytes6bytes6bytes6bytes63");
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
    delete tree;
    delete row_found_5;
    delete row_found_6;
    delete row_found_9;
    std::system("make cleandb");
}

BOOST_AUTO_TEST_CASE(add_column_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0 ; i < 10 ; i++){
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
    if (found_0){
        BOOST_CHECK(std::strncmp(found_0, expected_0, 61) == 0);
    }
    if (found_4){
        BOOST_CHECK(std::strncmp(found_4, expected_4, 61) == 0);
    }
    if (found_7){
        BOOST_CHECK(std::strncmp(found_7, expected_7, 61) == 0);
    }
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system("make cleandb");
}

BOOST_AUTO_TEST_CASE(remove_column_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0 ; i < 10 ; i++){
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
    if (found_0){
        BOOST_CHECK(std::strncmp(found_0, expected_0, 42) == 0);
    }
    if (found_4){
        BOOST_CHECK(std::strncmp(found_4, expected_4, 42) == 0);
    }
    if (found_7){
        BOOST_CHECK(std::strncmp(found_7, expected_7, 42) == 0);
    }
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system("make cleandb");
}

BOOST_AUTO_TEST_CASE(modify_column_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0 ; i < 10 ; i++){
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
    if (found_0){
        BOOST_CHECK(std::strncmp(found_0, expected_0, 62) == 0);
    }
    if (found_4){
        BOOST_CHECK(std::strncmp(found_4, expected_4, 62) == 0);
    }
    if (found_7){
        BOOST_CHECK(std::strncmp(found_7, expected_7, 62) == 0);
    }
    delete found_0;
    delete found_4;
    delete found_7;
    delete tree;
    std::system("make cleandb");
}

BOOST_AUTO_TEST_CASE(find_all_test){
    rsql::BTree *tree = new rsql::BTree();
    tree->add_column(rsql::Column::pkey_column(0));
    tree->add_column(rsql::Column::int_column(0, 4));
    tree->add_column(rsql::Column::date_column(0));
    tree->add_column(rsql::Column::char_column(0, 10));
    std::vector<char *> expected;
    char src[32 + 4 + 10 + 10 + 1] = "00000000000000000000000000000000444410101010101010101010";
    for (int i = 0 ; i < 10 ; i++){
        tree->insert_row(src);
        src[PKEY_COL_W - 1]++;
    }
    src[PKEY_COL_W - 1] = '5';
    char *expected_item = new char[32 + 4 + 10 + 10];
    std::memcpy(expected_item, src, 32 + 4 + 10 + 10);
    expected.push_back(expected_item);
    for (int i = 0 ; i < 3 ; i++){
        src[PKEY_COL_W]++;
        tree->insert_row(src);
        char *expected_i = new char[32 + 4 + 10 + 10];
        std::memcpy(expected_i, src, 32 + 4 + 10 + 10);
        expected.push_back(expected_i);
    }
    char key[PKEY_COL_W];
    std::memcpy(key, "00000000000000000000000000000005", PKEY_COL_W);
    std::vector<char *> alls = tree->find_all_row(key);
    BOOST_CHECK(alls.size() == 4);
    BOOST_CHECK(expected.size() == alls.size());
    for (int i = 0 ; i < alls.size() ; i++){
        bool found = false;
        for (int j = 0 ; j < expected.size() ; j++){
            if (strncmp(alls[i], alls[j], 32 + 4 + 10 + 10) == 0){
                found = true;
                break;
            }
        }
        BOOST_CHECK(found);
    }
    for (char *single : alls){
        delete[] single;
    }
    for (char *item : expected){
        delete[] item;
    }
    delete tree;
    std::system("make cleandb");
}
