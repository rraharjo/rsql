#ifndef TABLE_H
#define TABLE_H
#include <map>
#include <unordered_map>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>
#include <boost/multiprecision/cpp_int.hpp>
#include <queue>
#include "tree.h"
#define COMPOSITE_KEY_SIZE 2
#define TABLE_NAME_SIZE 256
#define COL_NAME_SIZE 128
#define DEF_KEY_COL_NAME "DEFAULT_KEY"
#define TABLE_FILE_NAME "table.rsql"

namespace rsql
{
    struct uintuint32
    {
        std::pair<uint32_t, uint32_t> pair;
        friend bool operator==(const uintuint32 &left, const uintuint32 &right);
    };
    struct PairComp
    {
        std::size_t operator()(const uintuint32 &p) const;
    };

    class Database;
    class Table
    {
    private:
        const Database *db;
        std::string table_name;
        std::unordered_map<std::string, uint32_t> col_name_indexes;
        uint32_t primary_tree_num;
        uint32_t max_tree_num;
        unsigned char next_default_key[DEFAULT_KEY_WIDTH];

        rsql::BTree *primary_tree;
        /**
         * @brief key is the index, value is the pointer to the tree. If a column does not have a tree, that column does not belong in this attribute
         * 
         */
        std::unordered_map<uint32_t, rsql::BTree *> optional_trees;
        std::unordered_map<uintuint32, rsql::BTree *, PairComp> composite_trees;

        bool changed;

        /**
         * @brief Construct a new Table object, does not necessarily write it to disk. Automatically set up the primary tree, numbered 1
         *
         * @param db
         * @param table_name
         */
        Table(const Database *db, const std::string table_name);
        std::vector<char *> find_row(const char *key, size_t col_idx, BTree *tree);

    public:
        /**
         * @brief Create a new table object.
         *
         * @throw std::invalid_argument if table already exists
         * @param db
         * @param table_name
         * @param col_names
         * @param columns
         * @return Table*
         */
        static Table *create_new_table(Database *db, const std::string table_name, std::vector<std::string> col_names = {}, std::vector<Column> columns = {});
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
         * @throw std::invalid_argument if the length of src in binary exceed the column width
         * @param key
         * @param col_name
         * @return std::vector<char *> all rows, dynamically allocated
         */
        void insert_row_text(const std::vector<std::string> &row);
        std::vector<char *> search_row_single_key(std::string key_col, const char *key, CompSymbol symbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        //TODO: test this function
        std::vector<char *> delete_row(std::string key_col, const char *key, CompSymbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        std::vector<char *> find_row_bin(const char *key, const std::string col_name);
        std::vector<char *> find_row_text(std::string key, const std::string col_name);
        std::vector<char *> find_row_col_comparison(const std::string col_name_1, const std::string col_name_2);
        /**
         * @brief Index column
         *
         * @throw std::invalid_argument if column can't be indexed
         * @param col_name
         */
        void index_column(const std::string col_name);
        /**
         * @brief UNTESTED!!! Index two column as composite. Key is represented as (col1 - col2)
         *
         * @throw std::invalid_argument if the columns can't be indexed
         * @param col_name_1
         * @param col_name_2
         */
        void index_composite_columns(const std::string col_name_1, const std::string col_name_2);
        /**
         * @brief Get the path, where this table is stored
         *
         * @return std::string the path
         */
        std::string get_path() const;
        void write_disk();
        friend class BTree;
        friend class BNode;
    };
}

#endif