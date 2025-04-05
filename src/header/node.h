#ifndef BNODE_H
#define BNODE_H
#include <regex>
#include <vector>
#include <fstream>
#include <cstring>
#include "column.h"
namespace rsql{
    class BTree;
}
namespace rsql
{
    class BNode
    {
    public:
        BTree *tree;
        // except the root, minimum of t-1 keys and a maximum of 2t - 1 keys
        std::vector<char *> keys;
        /*the number represent the file for the next node
        e.g. MYDB_int
        */
        std::vector<unsigned int> children;
        unsigned int node_num;
        size_t size;
        bool changed, leaf;

    public:
        /**
         * @brief
         * Read file_name file and store all the data inside keys
         * @param cols column format
         * @param file_name
         * @return BNode*
         */
        static BNode *read_disk(BTree *tree, std::string file_name);
        BNode(BTree *tree, unsigned int node_num);
        ~BNode();
        void insert(const char *row);
        void write_disk(std::string file_name);
    };
}
#endif