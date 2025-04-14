#ifndef BNODE_H
#define BNODE_H
#include <regex>
#include <vector>
#include <fstream>
#include <cstring>
#include <cstdint>
#include "column.h"
namespace rsql
{
    class BTree;
}
namespace rsql
{
    class BNode
    {
    public:
        /**
         * @brief return the first proper child idx
         * 
         * @param k 
         * @return int 
         */
        int first_child_idx(const char *k);
        /**
         * @brief return the last proper child idx
         * 
         * @param k 
         * @return int 
         */
        int last_child_idx(const char *k);
        /**
         * @brief Compare the first k byte, where k is the size of the first column
         *
         * @param k_1
         * @param k_2
         * @return < 0 if k_1 is less than k_2 ; > 0 if k_1 is larger than k_2
         */
        int compare_key(const char *k_1, const char *k_2);
        /**
         * @brief Shift keys to the right by 1 unit, starting at idx (inclusive)
         *
         * @param idx
         */
        void merge(size_t idx, BNode *c_i, BNode *c_j);
        /**
         * @brief case 1 of deleting an item: item is in this node, and this node is a leaf
         *
         * @param key
         */
        void delete_row_1(size_t idx);
        /**
         * @brief case 2 of deleting an item: item is in this node, and this node is not a leaf
         *
         * @param key
         */
        void delete_row_2(const char *key, size_t idx);
        /**
         * @brief case 3 of deleting an item: item is not in this node
         *
         * @param key
         * @param idx index of the child node
         */
        void delete_row_3(const char *key, size_t idx);
        void del_if_not_root();
        /**
         * @brief Delete this node along with the file
         *
         */
        void destroy();
        /**
         * @brief Split the children node c_i, which has to be this children at index idx
         *
         * @param idx position of the target child
         * @param c_i a pointer to the child node (c_i has to be in index idx)
         * @return a pointer to the newly created node, at position idx + 1
         */
        BNode *split_children(size_t idx, BNode *c_i);
        /**
         * @brief Match the column structure of the node to the tree
         * 
         */
        void match_columns();

    public:
        std::vector<Column> columns;
        uint32_t size;
        std::vector<char *> keys;
        std::vector<uint32_t> children;
        uint8_t leaf;
        
        
        uint32_t node_num;
        bool changed;
        BTree *tree;

    public:
        /**
         * @brief
         * Read file_name file and store all the data inside keys
         * @param cols column format
         * @param file_name
         * @return BNode*
         */
        static BNode *read_disk(BTree *tree, const std::string file_name);
        BNode(BTree *tree, uint32_t node_num);
        ~BNode();
        bool full();
        /**
         * @brief find the first row that matches the key
         *
         * @param key only compares the first k bytes, where k is the width of the first column
         * @return n bytes of char dynamically allocated, where n is the width of the table. If not found it returns nullptr
         */
        char *find(const char *key);
        /**
         * @brief Put all matching row to res
         * 
         * @param key 
         * @param res result vector
         */
        void find_all(const char *key, std::vector<char *>& res);
        void insert(const char *row);
        void delete_row(const char *key);
        void write_disk();

        friend class BTree;
    };
}
#endif