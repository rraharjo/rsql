#include "database.h"
#include "table.h"
namespace rsql
{
    Database::Database(const std::string db_name) : db_name(db_name)
    {
    }
    Database::~Database()
    {
    }

    std::string Database::get_path() const{
        return std::filesystem::path(ROOT_FOLDER) / this->db_name;
    }
    Database *Database::create_new_database(const std::string db_name)
    {
        std::string where = std::filesystem::path(ROOT_FOLDER) / db_name;
        if (std::filesystem::exists(where))
        {
            throw std::runtime_error("Database already exists");
            return nullptr;
        }
        std::filesystem::create_directory(where);
        Database *new_db = new Database(db_name);
        return new_db;
    }
    Database *Database::load_database(const std::string db_name)
    {
        std::string where = std::filesystem::path(ROOT_FOLDER) / db_name;
        if (!std::filesystem::exists(where))
        {
            throw std::runtime_error("Database does not exist");
        }
        Database *new_db = new Database(db_name);
        return new_db;
    }

    Table *Database::get_table(const std::string table_name)
    {
        return Table::load_table(this, table_name);
    }
    Table *Database::create_table(const std::string table_name)
    {
        return Table::create_new_table(this, table_name);
    }
    void Database::remove_table(const std::string table_name)
    {
        std::string where = std::filesystem::path(ROOT_FOLDER) / this->db_name / table_name;
        if (!std::filesystem::exists(where))
        {
            throw std::runtime_error("Table does not exist");
        }
        std::filesystem::remove_all(where);
    }
}