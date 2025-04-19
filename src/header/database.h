#ifndef DATABASE_RSQL_H
#define DATABASE_RSQL_H
#include <string>
#include "table.h"
#define ROOT_FOLDER "/home/raharjo/project/database/rsql"
namespace rsql{
    class Table;
    class BTree;
    class Database{
    private:
        std::string db_name;
        Database(const std::string db_name);
    public:
        /**
         * @brief Create a new database object and write it to disk
         *
         * @throw std::invalid_argument if database already exists
         * @param db_name
         * @return Database*
         */
        static Database *create_new_database(const std::string db_name);
        /**
         * @brief load existing database from disk
         *
         * @throw std::invalid_argument if database doesn't already exist
         * @param db_name
         * @return Database*
         */
        static Database *load_database(const std::string db_name);
        std::string get_path() const;
        /**
         * @brief Get the table object with matching name
         *
         * @throw std::runtime_error if failed during system call
         * @throw std::invalid_argument if table_name doesn't exist
         * @param table_name
         * @return Table*
         */
        Table *get_table(const std::string table_name);
        /**
         * @brief Create a new table object and write it to disk
         *
         * @throw std::invalid_argument if table already exists
         * @param table_name
         * @return Table*
         */
        Table *create_table(const std::string table_name);
        /**
         * @brief remove table named table_name
         * 
         * @throw std::invalid_argument if table_name doesn't exist
         * @param table_name 
         */
        void remove_table(const std::string table_name);
        ~Database();
        friend class Table;
        friend class BTree;
        friend class BNode;
    };
}
#endif