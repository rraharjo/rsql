#include "database.h"
#include "table.h"
static std::string get_last_entry_in_path(const std::string &path)
{
    int l = path.length() - 1;
    while (l >= 0)
    {
        if (path[l] == '/')
            return path.substr(l + 1, path.length() - l);
        l--;
    }
    return path;
}
namespace rsql
{
    Database::Database(const std::string db_name) : db_name(db_name)
    {
    }
    Database::~Database()
    {
    }

    std::string Database::get_path() const
    {
        return std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / this->db_name;
    }
    Database *Database::create_new_database(const std::string db_name)
    {
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / db_name;
        if (std::filesystem::exists(where))
        {
            throw std::invalid_argument("Database already exists");
            return nullptr;
        }
        std::filesystem::create_directory(where);
        Database *new_db = new Database(db_name);
        return new_db;
    }
    Database *Database::load_database(const std::string db_name)
    {
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / db_name;
        if (!std::filesystem::exists(where))
        {
            throw std::invalid_argument("Database does not exist");
        }
        Database *new_db = new Database(db_name);
        return new_db;
    }
    std::vector<std::string> Database::list_databases()
    {
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER);
        std::vector<std::string> to_ret;
        for (const auto &entry : std::filesystem::directory_iterator(where))
        {
            if (entry.is_directory())
            {
                to_ret.push_back(get_last_entry_in_path(entry.path()));
            }
        }
        return to_ret;
    }
    void Database::delete_database(const std::string db_name)
    {
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / db_name;
        if (!std::filesystem::exists(where))
        {
            throw std::invalid_argument("Database does not exist");
        }
        std::filesystem::remove_all(where);
    }
    std::vector<std::string> Database::list_tables()
    {
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / this->db_name;
        std::vector<std::string> to_ret;
        for (const auto &entry : std::filesystem::directory_iterator(where))
        {
            if (entry.is_directory())
            {
                to_ret.push_back(get_last_entry_in_path(entry.path()));
            }
        }
        return to_ret;
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
        std::string where = std::filesystem::path(DATABASE_FILE_ROOT_FOLDER) / this->db_name / table_name;
        if (!std::filesystem::exists(where))
        {
            throw std::invalid_argument("Table does not exist");
            return;
        }
        std::filesystem::remove_all(where);
    }
}