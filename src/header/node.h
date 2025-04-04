#ifndef BNODE_H
#define BNODE_H
#include <vector>
#include <fstream>
#include <cstring>
#include "column.h"
namespace rsql
{
    class BNode
    {
    private:
        std::vector<Column> cols;
        // except the root, minimum of t-1 keys and a maximum of 2t - 1 keys
        std::vector<char *> keys;
        /*the number represent the file for the next node
        e.g. MYDB_int
        */
        std::vector<int> children;
        size_t size;
        // degree
        size_t t;

    public:
        /**
         * @brief
         * Read file_name file and store all the data inside keys
         * @param cols column format
         * @param file_name
         * @return BNode*
         */
        static BNode *read_disk(std::vector<Column> &cols, size_t t, std::string file_name);
        BNode(std::vector<Column> &cols, size_t t);
        ~BNode();
        /**
         * @brief Get the width of the row in bytes
         * @return size_t
         */
        size_t get_width();
        size_t get_size();
        size_t get_t();
        void insert(const std::string row);
        void write_disk(std::string file_name);
    };
}
#endif