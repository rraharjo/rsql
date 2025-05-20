#include "driver.h"
bool inline valid_db(rsql::Database *db);
std::vector<std::string> split(const std::string &str, const std::string &delimiter);
namespace rsql
{
    Parser::Parser()
    {
    }
    Parser::~Parser()
    {
        delete this->db;
    }
    void Parser::list_db()
    {
        std::vector<std::string> dbs = Database::list_databases();
        for (const std::string &db : dbs)
        {
            std::cout << db << std::endl;
        }
    }
    bool Parser::list_tables()
    {
        if (!valid_db(this->db))
        {
            return false;
        }
        std::vector<std::string> tables = this->db->list_tables();
        for (const std::string &table : tables)
        {
            std::cout << table << std::endl;
        }
        return true;
    }
    bool Parser::create_db(const std::string db_name)
    {
        try
        {
            Database *temp = Database::create_new_database(db_name);
            delete temp;
            return true;
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return false;
        }
    }
    bool Parser::connect_database(const std::string db_name)
    {
        try
        {
            Database *temp = Database::load_database(db_name);
            if (this->db)
            {
                delete this->db;
            }
            this->db = temp;
            return true;
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return false;
        }
    }
    bool Parser::delete_db(const std::string db_name)
    {
        try
        {
            Database::delete_database(db_name);
            return true;
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return false;
        }
    }
    Table *Parser::add_table(const std::string table_name)
    {
        if (!valid_db(this->db))
        {
            return nullptr;
        }
        try
        {
            return this->db->create_table(table_name);
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return nullptr;
        }
    }
    Table *Parser::get_table(const std::string table_name)
    {
        if (!valid_db(this->db))
        {
            return nullptr;
        }
        try
        {
            return this->db->get_table(table_name);
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return nullptr;
        }
    }
    bool Parser::delete_table(const std::string table_name)
    {
        if (!valid_db(this->db))
        {
            return false;
        }
        try
        {
            this->db->remove_table(table_name);
            return true;
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return false;
        }
    }
    void Parser::routine()
    {
        std::string input;
        while (input != "end")
        {
            std::cout << "> ";
            std::getline(std::cin, input);
            std::vector<std::string> token = split(input, " ");
            if (token.size() < 2)
            {
                std::cout << "Failed" << std::endl;
            }
            else
            {
                if (token[0] == "connect")
                {
                    if (this->connect_database(token[1]))
                    {
                        std::cout << "Database Connected" << std::endl;
                    };
                }
                else if (token[0] == "createdb")
                {
                    if (this->create_db(token[1]))
                    {
                        std::cout << "Database Created" << std::endl;
                    };
                }
                else if (token[0] == "createtable")
                {
                    if (this->add_table(token[1]))
                    {
                        std::cout << "Table Created" << std::endl;
                    }
                }
                else if (token[0] == "deletetable")
                {
                    if (this->delete_table(token[1]))
                    {
                        std::cout << "Table Deleted" << std::endl;
                    }
                }
                else if (token[0] == "deletedb")
                {
                    if (this->delete_db(token[1]))
                    {
                        std::cout << "Database Deleted" << std::endl;
                    }
                }
            }
        }
    }
}
bool inline valid_db(rsql::Database *db)
{
    if (db == nullptr)
    {
        std::cout << "Not connected to any db" << std::endl;
        return false;
    }
    return true;
}
std::vector<std::string> split(const std::string &str, const std::string &delimiter)
{
    std::vector<std::string> tokens;
    size_t pos = 0;
    size_t last = 0;

    while ((pos = str.find(delimiter, last)) != std::string::npos)
    {
        tokens.push_back(str.substr(last, pos - last));
        last = pos + delimiter.length();
    }

    // Don't forget the last token
    tokens.push_back(str.substr(last));

    return tokens;
}