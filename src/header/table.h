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

    private:
        std::string table_name;
        std::unordered_map<std::string, uint32_t> col_name_indexes;
        uint32_t primary_tree_num;
        uint32_t max_tree_num;
        unsigned char next_default_key[DEFAULT_KEY_WIDTH];

        const Database *db;
        bool changed;
        rsql::BTree *primary_tree;
        /**
         * @brief key is the index, value is the pointer to the tree. If a column does not have a tree, that column does not belong in this attribute
         *
         */
        std::unordered_map<uint32_t, rsql::BTree *> optional_trees;
        

    private: 
        /**
         * @brief Construct a new Table object, does not necessarily write it to disk. Automatically set up the primary tree, numbered 1
         *
         * @param db
         * @param table_name
         */
        Table(const Database *db, const std::string table_name);

    public:
        ~Table();
        inline size_t get_width() const {return this->primary_tree->get_width(); }
        /**
         * @brief Convert a vector of values (in text format) to binary format. Number of values has to match number of columns. Does not count default key column
         *
         * @param values
         * @throw std::invalid_argument if the number of entries does not match the number of columns
         * @return char* dynamically allocated. Ownership belongs to caller
         */
        char *convert_texts_to_char_stream(const std::vector<std::string> &values);
        /**
         * @brief Convert a stream to text format. Argument should be store the whole row information
         * 
         * @param stream 
         * @return std::vector<std::string> 
         */
        std::vector<std::string> convert_char_stream_to_texts(const char *const stream);
        Column get_column(std::string col_name);
        std::vector<std::pair<std::string, Column>> get_columns();
        size_t get_preceding_length(const std::string col_name) const;
        /**
         * @brief Get the width of the column with matching name
         *
         * @throw std::invalid_argument if col_name doesn't exist in this table
         * @param col_name
         * @return size_t
         */
        size_t get_col_width(const std::string col_name) const;
        /**
         * @brief Add new column to the table
         *
         * @param name
         * @param col
         */
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
        /**
         * @brief Search all the rows within this table that matches the criteria provided
         *
         * @param key_col Key column for comparison
         * @param key value of key column - binary format
         * @param symbol
         * @param comparison
         * @return std::vector<char *>
         */
        std::vector<char *> search_row_single_key(std::string key_col, const char *key, CompSymbol symbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        /**
         * @brief Delete all the rows within this table that matches the criteria provided
         *
         * @param key_col
         * @param key key to find - binary format
         * @param comparison
         * @return std::vector<char *>
         */
        std::vector<char *> delete_row(std::string key_col, const char *key, CompSymbol = CompSymbol::EQ, Comparison *comparison = nullptr);
        /**
         * @brief Index column
         *
         * @throw std::invalid_argument if column can't be indexed
         * @param col_name
         */
        void index_column(const std::string col_name);
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