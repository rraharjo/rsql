#ifndef DRIVER_H
#define DRIVER_H
#include <iostream>
#include "database.h"
#include "sql_parser.h"

namespace rsql
{
    class Driver
    {
    private:
        rsql::Database *db = nullptr;
        std::string current_instruction;

    public:
        Driver();
        ~Driver();
        void list_db();
        bool list_tables();
        bool create_db(const std::string db_name);
        bool connect_database(const std::string db_name);
        bool delete_db(const std::string db_name);
        Table *add_table(const std::string table_name);
        Table *get_table(const std::string table_name);
        bool delete_table(const std::string table_name);
        void routine();

        friend class SQLParser;
    };
}
#endif