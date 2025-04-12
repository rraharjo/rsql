#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "tree.h"

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
    system("make cleandb");
}