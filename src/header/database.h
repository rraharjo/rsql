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
        static Database *create_new_database(const std::string db_name);
        static Database *load_database(const std::string db_name);
        std::string get_path() const;
        Table *get_table(const std::string table_name);
        Table *create_table(const std::string table_name);
        void remove_table(const std::string table_name);
        ~Database();
        friend class Table;
        friend class BTree;
        friend class BNode;
    };
}
#endif