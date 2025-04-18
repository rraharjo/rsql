#ifndef TABLE_H
#define TABLE_H
#include <map>
#include <unordered_map>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include "tree.h"
#define TABLE_NAME_SIZE 256
#define COL_NAME_SIZE 128
#define TABLE_FILE_NAME "table.rsql"
namespace rsql
{
    class Database;
    class Table
    {
    private:
        const Database *db;
        std::string table_name;
        std::unordered_map<std::string , uint32_t> col_name_indexes;
        rsql::BTree *primary_tree;
        bool changed;

        Table(const Database *db, const std::string table_name);
    public:
        static Table *create_new_table(Database *db, const std::string table_name);
        static Table *load_table(Database *db, const std::string table_name);
        ~Table();
        std::string get_path() const;
        void add_column(const std::string name, const rsql::Column col);
        void remove_column(const std::string col_name);
        void write_disk();
        friend class BTree;
        friend class BNode;
    };
}

#endif