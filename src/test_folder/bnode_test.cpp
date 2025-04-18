#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "tree.h"
std::string clean_this_cache = "make cleanthiscache";
BOOST_AUTO_TEST_CASE(merge_node_test){
    size_t structure_1_w = PKEY_COL_W;
    std::vector<rsql::Column> structure_1;
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    rsql::BTree *tree = new rsql::BTree();
    for (rsql::Column &c : structure_1){
        tree->add_column(c);
    }
    char key[PKEY_COL_W + 1] = "00000000000000000000000000000000";
    rsql::BNode *parent = new rsql::BNode(tree, 1);
    parent->keys[0] = new char[PKEY_COL_W];
    parent->keys[1] = new char[PKEY_COL_W];
    parent->size = 2;
    key[PKEY_COL_W - 1] = '1';
    std::memcpy(parent->keys[0], key, PKEY_COL_W);
    key[PKEY_COL_W - 1] = '3';
    std::memcpy(parent->keys[1], key, PKEY_COL_W);

    rsql::BNode *c_1 = new rsql::BNode(tree, 2);
    c_1->keys[0] = new char[PKEY_COL_W];
    c_1->size = 1;
    key[PKEY_COL_W - 1] = '0';
    std::memcpy(c_1->keys[0], key, PKEY_COL_W);

    rsql::BNode *c_2 = new rsql::BNode(tree, 3);
    c_2->keys[0] = new char[PKEY_COL_W];
    c_2->size = 1;
    key[PKEY_COL_W - 1] = '2';
    std::memcpy(c_2->keys[0], key, PKEY_COL_W);

    rsql::BNode *c_3 = new rsql::BNode(tree, 4);
    c_3->keys[0] = new char[PKEY_COL_W];
    c_3->size = 1;
    key[PKEY_COL_W - 1] = '4';
    std::memcpy(c_3->keys[0], key, PKEY_COL_W);

    parent->merge(0, c_1, c_2);

    BOOST_CHECK(parent->size == 1);
    BOOST_CHECK(strncmp(parent->keys[0], "00000000000000000000000000000003", PKEY_COL_W) == 0);
    BOOST_CHECK(c_1->size == 3);
    BOOST_CHECK(strncmp(c_1->keys[0], "00000000000000000000000000000000", PKEY_COL_W) == 0);
    BOOST_CHECK(strncmp(c_1->keys[1], "00000000000000000000000000000001", PKEY_COL_W) == 0);
    BOOST_CHECK(strncmp(c_1->keys[2], "00000000000000000000000000000002", PKEY_COL_W) == 0);
    BOOST_CHECK(c_3->size == 1);
    BOOST_CHECK(strncmp(c_3->keys[0], "00000000000000000000000000000004", PKEY_COL_W) == 0);
    delete tree;
    delete c_1;
    delete c_3;
    std::system(clean_this_cache.c_str());
}
BOOST_AUTO_TEST_CASE(match_column_test){
    size_t structure_1_w = PKEY_COL_W + 4 + 10;
    std::vector<rsql::Column> structure_1;
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::PKEY, 0));
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::INT, 4));
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    rsql::BTree *tree = new rsql::BTree();
    for (rsql::Column &c : structure_1){
        tree->add_column(c);
    }
    rsql::BNode *node = new rsql::BNode(tree, 0);
    BOOST_CHECK(node->columns == tree->columns);
    node->keys[0] = new char[structure_1_w];
    node->keys[1] = new char[structure_1_w];
    char item1[46];
    std::memset(item1, 'p', 32);
    std::memset(item1 + 32, 'i', 4);
    std::memset(item1 + 32 + 4, 'c', 10);
    char item2[46];
    std::memset(item2, 'p', 32);
    std::memset(item2 + 32, 'I', 4);
    std::memset(item2 + 32 + 4, 'C', 10);
    std::memcpy(node->keys[0], item1, structure_1_w);
    std::memcpy(node->keys[1], item2, structure_1_w);
    node->size = 2;
    rsql::Column added = rsql::Column::get_column(0, rsql::DataType::INT, 5);
    tree->add_column(added);
    tree->remove_column(1);
    node->match_columns();
    char match1[structure_1_w + 5 - 4];
    std::memset(match1, 0, structure_1_w + 5 - 4);
    std::memset(match1, 'p', 32);
    std::memset(match1 + 32, 'c', 10);
    char match2[structure_1_w + 5 - 4];
    std::memset(match2, 0, structure_1_w + 5 - 4);
    std::memset(match2, 'p', 32);
    std::memset(match2 + 32, 'C', 10);
    BOOST_CHECK(std::strncmp(node->keys[0], match1, structure_1_w + 5 - 4) == 0);
    BOOST_CHECK(std::strncmp(node->keys[1], match2, structure_1_w + 5 - 4) == 0);
    std::system(clean_this_cache.c_str());
}