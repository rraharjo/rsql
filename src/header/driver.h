#ifndef DRIVER_H
#define DRIVER_H
#include <iostream>
#include "database.h"
#include "sql_parser.h"
typedef std::shared_ptr<rsql::Table> tableptr;
namespace rsql
{
    class Driver
    {
    private:
        rsql::Database *db = nullptr;
        std::string current_instruction;
        std::unordered_map<std::string, tableptr> tables;

    public:
        Driver();
        ~Driver();
        void list_db();
        void list_tables();
        void create_db(const std::string db_name);
        void connect_database(const std::string db_name);
        void delete_db(const std::string db_name);
        tableptr add_table(const std::string table_name);
        tableptr get_table(const std::string table_name);
        void delete_table(const std::string table_name);
        void routine();
        void cleanup();

        friend class SQLParser;
    };
}
#endif