#ifndef TABLE_H
#define TABLE_H
#include <map>
#include <unordered_map>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <boost/multiprecision/cpp_int.hpp>
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
        std::unordered_map<std::string, uint32_t> col_name_indexes;
        rsql::BTree *primary_tree;
        bool changed;

        Table(const Database *db, const std::string table_name);

    public:
        /**
         * @brief Create a new table object.
         *
         * @throw std::invalid_argument if table already exists
         * @param db
         * @param table_name
         * @return Table*
         */
        static Table *create_new_table(Database *db, const std::string table_name);
        /**
         * @brief load existing table from db
         *
         * @throw std::invalid_argument if table_name doesn't already exist
         * @throw std::runtime_error if failed during system call
         * @param db
         * @param table_name
         * @return Table*
         */
        static Table *load_table(Database *db, const std::string table_name);
        ~Table();
        /**
         * @brief Get the width of the table
         *
         * @return size_t
         */
        size_t get_width() const;
        /**
         * @brief Get the width of the column with matching name
         *
         * @throw std::invalid_argument if col_name doesn't exist in this table
         * @param col_name
         * @return size_t
         */
        size_t get_col_width(const std::string col_name) const;
        /**
         * @brief Get the path, where this table is stored
         *
         * @return std::string the path
         */
        std::string get_path() const;
        void add_column(const std::string name, const rsql::Column col);
        /**
         * @brief remove a column named col_name
         *
         * @throw std::invalid_argument when col_name doesn't exist
         * @param col_name
         */
        void remove_column(const std::string col_name);
        /**
         * @brief insert a new row in binary mode. Will insert the first n bytes, n being the width of the table
         * 
         * @param row 
         */
        void insert_row_bin(const char *row);
        /**
         * @brief find all rows with where the col_name column has matching values
         * 
         * @throw std::invalid_argument if this table does not have column col_name
         * @param key 
         * @param col_name 
         * @return std::vector<char *> all rows, dynamically allocated
         */
        void insert_row_text(const std::vector<std::string> &row);
        std::vector<char *> find_row(const char *key, const std::string col_name);
        void write_disk();
        friend class BTree;
        friend class BNode;
    };
}

#endif