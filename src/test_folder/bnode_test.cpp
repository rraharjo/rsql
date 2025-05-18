#define BOOST_TEST_MODULE MyTestModule
#include <boost/test/included/unit_test.hpp>
#include "tree.h"
std::string clean_this_cache = "make cleancache";
BOOST_AUTO_TEST_CASE(merge_node_test)
{
    std::vector<rsql::Column> structure_1;
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::DEFAULT_KEY, 0));
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    for (rsql::Column &c : structure_1)
    {
        tree->add_column(c);
    }
    char key[DEFAULT_KEY_WIDTH + 1] = "00000000000000000000000000000000";
    rsql::BNode *parent = new rsql::BNode(tree, 1);
    parent->keys[0] = new char[DEFAULT_KEY_WIDTH];
    parent->keys[1] = new char[DEFAULT_KEY_WIDTH];
    parent->size = 2;
    key[DEFAULT_KEY_WIDTH - 1] = '1';
    std::memcpy(parent->keys[0], key, DEFAULT_KEY_WIDTH);
    key[DEFAULT_KEY_WIDTH - 1] = '3';
    std::memcpy(parent->keys[1], key, DEFAULT_KEY_WIDTH);

    rsql::BNode *c_1 = new rsql::BNode(tree, 2);
    c_1->keys[0] = new char[DEFAULT_KEY_WIDTH];
    c_1->size = 1;
    key[DEFAULT_KEY_WIDTH - 1] = '0';
    std::memcpy(c_1->keys[0], key, DEFAULT_KEY_WIDTH);

    rsql::BNode *c_2 = new rsql::BNode(tree, 3);
    c_2->keys[0] = new char[DEFAULT_KEY_WIDTH];
    c_2->size = 1;
    key[DEFAULT_KEY_WIDTH - 1] = '2';
    std::memcpy(c_2->keys[0], key, DEFAULT_KEY_WIDTH);

    rsql::BNode *c_3 = new rsql::BNode(tree, 4);
    c_3->keys[0] = new char[DEFAULT_KEY_WIDTH];
    c_3->size = 1;
    key[DEFAULT_KEY_WIDTH - 1] = '4';
    std::memcpy(c_3->keys[0], key, DEFAULT_KEY_WIDTH);

    parent->merge(0, c_1, c_2);

    BOOST_CHECK(parent->size == 1);
    BOOST_CHECK(strncmp(parent->keys[0], "00000000000000000000000000000003", DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(c_1->size == 3);
    BOOST_CHECK(strncmp(c_1->keys[0], "00000000000000000000000000000000", DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(strncmp(c_1->keys[1], "00000000000000000000000000000001", DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(strncmp(c_1->keys[2], "00000000000000000000000000000002", DEFAULT_KEY_WIDTH) == 0);
    BOOST_CHECK(c_3->size == 1);
    BOOST_CHECK(strncmp(c_3->keys[0], "00000000000000000000000000000004", DEFAULT_KEY_WIDTH) == 0);
    delete tree;
    delete c_1;
    delete c_3;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(match_column_test)
{
    size_t structure_1_w = DEFAULT_KEY_WIDTH + 4 + 10;
    std::vector<rsql::Column> structure_1;
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::DEFAULT_KEY, 0));
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::UINT, 4));
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::CHAR, 10));
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    for (rsql::Column &c : structure_1)
    {
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
    rsql::Column added = rsql::Column::get_column(0, rsql::DataType::UINT, 5);
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
    delete node;
    delete tree;
    std::system(clean_this_cache.c_str());
}

BOOST_AUTO_TEST_CASE(find_first_idx_test)
{
    std::vector<rsql::Column> structure_1;
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::DEFAULT_KEY, 0));
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    for (rsql::Column &c : structure_1)
    {
        tree->add_column(c);
    }
    if (DEGREE != 128)//Tested on degree 128
    {
        return;
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    rsql::BNode *node = new rsql::BNode(tree, 0);
    size_t cur_idx = 0;
    for (; cur_idx < 5; cur_idx++)
    { // 0 to 4
        node->keys[cur_idx] = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(node->keys[cur_idx], key, DEFAULT_KEY_WIDTH);
        key[DEFAULT_KEY_WIDTH - 1]++;
    }
    for (; cur_idx < 8; cur_idx++)
    { // 5 to 7
        node->keys[cur_idx] = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(node->keys[cur_idx], key, DEFAULT_KEY_WIDTH);
    }
    for (; cur_idx < 10; cur_idx++)
    { // 8 to 9
        key[DEFAULT_KEY_WIDTH - 1]++;
        node->keys[cur_idx] = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(node->keys[cur_idx], key, DEFAULT_KEY_WIDTH);
    }
    key[DEFAULT_KEY_WIDTH - 1] = 5;
    node->size = 10;
    // 0 1 2 3 4 5 5 5 6 7
    // 0 1 2 3 4 5 6 7 8 9
    size_t lt, leq, eq, geq, gt;
    lt = node->first_child_idx(key, rsql::CompSymbol::LT);
    leq = node->first_child_idx(key, rsql::CompSymbol::LEQ);
    eq = node->first_child_idx(key, rsql::CompSymbol::EQ);
    geq = node->first_child_idx(key, rsql::CompSymbol::GEQ);
    gt = node->first_child_idx(key, rsql::CompSymbol::GT);
    BOOST_CHECK(lt == 0);
    BOOST_CHECK(leq == 0);
    BOOST_CHECK(eq == 5);
    BOOST_CHECK(geq == 5);
    BOOST_CHECK(gt == 8);
    delete node;
    delete tree;
}

BOOST_AUTO_TEST_CASE(find_last_idx_test)
{
    std::vector<rsql::Column> structure_1;
    structure_1.push_back(rsql::Column::get_column(0, rsql::DataType::DEFAULT_KEY, 0));
    rsql::BTree *tree = rsql::BTree::create_new_tree();
    for (rsql::Column &c : structure_1)
    {
        tree->add_column(c);
    }
    if (DEGREE != 128)
    {
        return;
    }
    char key[DEFAULT_KEY_WIDTH];
    std::memset(key, 0, DEFAULT_KEY_WIDTH);
    rsql::BNode *node = new rsql::BNode(tree, 0);
    size_t cur_idx = 0;
    for (; cur_idx < 5; cur_idx++)
    { // 0 to 4
        node->keys[cur_idx] = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(node->keys[cur_idx], key, DEFAULT_KEY_WIDTH);
        key[DEFAULT_KEY_WIDTH - 1]++;
    }
    for (; cur_idx < 8; cur_idx++)
    { // 5 to 7
        node->keys[cur_idx] = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(node->keys[cur_idx], key, DEFAULT_KEY_WIDTH);
    }
    for (; cur_idx < 10; cur_idx++)
    { // 8 to 9
        key[DEFAULT_KEY_WIDTH - 1]++;
        node->keys[cur_idx] = new char[DEFAULT_KEY_WIDTH];
        std::memcpy(node->keys[cur_idx], key, DEFAULT_KEY_WIDTH);
    }
    key[DEFAULT_KEY_WIDTH - 1] = 5;
    node->size = 10;
    // 0 1 2 3 4 5 5 5 6 7
    // 0 1 2 3 4 5 6 7 8 9
    size_t lt, leq, eq, geq, gt;
    lt = node->last_child_idx(key, rsql::CompSymbol::LT);
    leq = node->last_child_idx(key, rsql::CompSymbol::LEQ);
    eq = node->last_child_idx(key, rsql::CompSymbol::EQ);
    geq = node->last_child_idx(key, rsql::CompSymbol::GEQ);
    gt = node->last_child_idx(key, rsql::CompSymbol::GT);
    BOOST_CHECK(lt == 5);
    BOOST_CHECK(leq == 8);
    BOOST_CHECK(eq == 8);
    BOOST_CHECK(geq == 10);
    BOOST_CHECK(gt == 10);
    delete node;
    delete tree;
}