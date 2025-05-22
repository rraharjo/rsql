#include "driver.h"
static bool inline valid_db(rsql::Database *db);
static std::vector<std::string> split(const std::string &str, const std::string &delimiter);
static std::string get_first_token(const std::string &str);
namespace rsql
{
    Driver::Driver()
    {
    }
    Driver::~Driver()
    {
        delete this->db;
    }
    void Driver::list_db()
    {
        std::vector<std::string> dbs = Database::list_databases();
        for (const std::string &db : dbs)
        {
            std::cout << db << std::endl;
        }
    }
    bool Driver::list_tables()
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
    bool Driver::create_db(const std::string db_name)
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
    bool Driver::connect_database(const std::string db_name)
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
    bool Driver::delete_db(const std::string db_name)
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
    Table *Driver::add_table(const std::string table_name)
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
    Table *Driver::get_table(const std::string table_name)
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
    bool Driver::delete_table(const std::string table_name)
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
    void Driver::routine()
    {
        std::string input;
        while (input != "end")
        {
            std::getline(std::cin, input);
            std::string main_token = get_first_token(input);
            try
            {
                if (main_token == CREATE)
                {
                    CreateParser parser(input);
                    parser.parse();
                    if (parser.create_db)
                    {
                        this->create_db(parser.get_target_name());
                    }
                    else
                    {
                        std::unique_ptr<Table> table;
                        table.reset(this->add_table(parser.get_target_name()));
                    }
                }
                else if (main_token == INSERT)
                {
                    InsertParser parser(input);
                    parser.parse();
                    std::unique_ptr<Table> table;
                    table.reset(this->get_table(parser.get_target_name()));
                    for (const std::vector<std::string> &row : parser.row_values)
                    {
                        table->insert_row_text(row);
                    }
                }
                else if (main_token == SELECT)
                {
                    SearchParser parser(input);
                    parser.parse();
                    std::unique_ptr<Table> table;
                    table.reset(this->get_table(parser.get_target_name()));
                    parser.extract_conditions(table.get());
                    std::vector<char *> result = table->search_row_single_key(parser.main_col_name, parser.main_val, parser.main_symbol, parser.comparison);
                    for (const char *res : result)
                    {
                        delete[] res;
                    }
                }
                else if (main_token == DELETE)
                {
                    DeleteParser parser(input);
                    parser.parse();
                    std::unique_ptr<Table> table;
                    table.reset(this->get_table(parser.get_target_name()));
                    parser.extract_conditions(table.get());
                    std::vector<char *> result = table->delete_row(parser.main_col_name, parser.main_val, parser.main_symbol, parser.comparison);
                    for (const char *res : result)
                    {
                        delete[] res;
                    }
                }
                else
                {
                    throw std::invalid_argument("Unknown command " + main_token);
                }
            }
            catch (const std::invalid_argument &e)
            {
                std::cout << e.what() << std::endl;
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

std::string get_first_token(const std::string &str)
{
    size_t l = 0;
    size_t r = 0;
    while (r < str.length() && str[r] == ' ')
    {
        r++;
    }
    l = r;
    while (r < str.length() && str[r] != ' ')
    {
        r++;
    }
    return str.substr(l, r - l);
}