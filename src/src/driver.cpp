#include "driver.h"
static bool inline valid_db(rsql::Database *db);
static std::vector<std::string> split(const std::string &str, const std::string &delimiter);
static void print_vv(const std::vector<std::vector<std::string>> &vv);
static inline void to_lower_case(std::string &str);
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
    void Driver::list_tables()
    {
        if (!valid_db(this->db))
            return;
        std::vector<std::string> tables = this->db->list_tables();
        for (const std::string &table : tables)
            std::cout << table << std::endl;
    }
    void Driver::create_db(const std::string db_name)
    {
        Database *temp = Database::create_new_database(db_name);
        delete temp;
    }
    void Driver::connect_database(const std::string db_name)
    {
        Database *temp = Database::load_database(db_name);
        if (this->db)
            this->cleanup();
        this->db = temp;
    }
    void Driver::delete_db(const std::string db_name)
    {
        Database::delete_database(db_name);
    }
    tableptr Driver::add_table(const std::string table_name)
    {
        if (!valid_db(this->db))
        {
            return nullptr;
        }
        try
        {
            Table *new_table = this->db->create_table(table_name);
            tableptr to_add;
            to_add.reset(new_table);
            this->tables.insert({table_name, to_add});
            return to_add;
        }
        catch (const std::invalid_argument &e)
        {
            std::cout << e.what() << std::endl;
            return nullptr;
        }
    }
    tableptr Driver::get_table(const std::string table_name)
    {
        if (!valid_db(this->db))
        {
            throw std::invalid_argument("Invalid table");
        }
        auto table_it = this->tables.find(table_name);
        if (table_it == this->tables.end())
        {
            Table *to_ret = this->db->get_table(table_name);
            tableptr to_add;
            to_add.reset(to_ret);
            this->tables.insert({"table_name", to_add});
            return to_add;
        }
        else
        {
            return table_it->second;
        }
    }
    void Driver::delete_table(const std::string table_name)
    {
        if (!valid_db(this->db))
            return;
        this->db->remove_table(table_name);
    }
    void Driver::routine()
    {
        std::string input;
        while (true)
        {
            std::cout << "RSQL> ";
            std::getline(std::cin, input);
            if (input == "exit")
            {
                break;
            }
            std::vector<std::string> tokens = split(input, " ");
            std::string main_token = tokens[0];
            to_lower_case(main_token);
            try
            {
                if (main_token == CREATE)
                {
                    if (tokens[1] == DATABASE)
                    {
                        CreateDBParser parser(input);
                        parser.parse();
                        this->create_db(parser.get_target_name());
                    }
                    else if (tokens[1] == TABLE)
                    {
                        CreateTableParser parser(input);
                        parser.parse();
                        tableptr table = this->add_table(parser.get_target_name());
                        for (const std::pair<std::string, Column> &p : parser.get_columns())
                            table->add_column(p.first, p.second);
                    }
                    else
                    {
                        throw std::invalid_argument("Unknown command");
                    }
                }
                else if (main_token == CONNECT)
                {
                    ConnectParser parser(input);
                    parser.parse();
                    this->connect_database(parser.get_target_name());
                }
                else if (main_token == INSERT)
                {
                    InsertParser parser(input);
                    parser.parse();
                    tableptr table = this->get_table(parser.get_target_name());
                    if (!table)
                        continue;
                    for (const std::vector<std::string> &row : parser.row_values)
                        table->insert_row_text(row);
                }
                else if (main_token == SELECT)
                {
                    SearchParser parser(input);
                    parser.parse();
                    tableptr table = this->get_table(parser.get_target_name());
                    parser.extract_comparisons(table.get());
                    std::vector<char *> result = table->search_row_single_key(parser.main_col_name, parser.main_val, parser.main_symbol, parser.comparison);
                    std::vector<std::vector<std::string>> result_str;
                    for (const char *res : result)
                    {
                        result_str.push_back(table->convert_char_stream_to_texts(res));
                        delete[] res;
                    }
                    print_vv(result_str);
                }
                else if (main_token == DELETE)
                {
                    DeleteParser parser(input);
                    parser.parse();
                    tableptr table = this->get_table(parser.get_target_name());
                    parser.extract_comparisons(table.get());
                    std::vector<char *> result = table->delete_row(parser.main_col_name, parser.main_val, parser.main_symbol, parser.comparison);
                    std::vector<std::vector<std::string>> result_str;
                    for (const char *res : result)
                    {
                        result_str.push_back(table->convert_char_stream_to_texts(res));
                        delete[] res;
                    }
                    print_vv(result_str);
                }
                else if (main_token == ALTER)
                {
                    std::vector<std::string> tokens = split(input, " ");
                    AlterTableParser parser(input);
                    parser.parse();
                    tableptr table = this->get_table(parser.get_target_name());
                    if (tokens[3] == ADD)
                    {
                        for (const std::pair<std::string, Column> &p : parser.get_columns())
                            table->add_column(p.first, p.second);
                    }
                    else if (tokens[3] == DELETE)
                        for (const std::string &col_name : parser.get_col_names())
                            table->remove_column(col_name);
                    else if (tokens[3] == INDEX_COL)
                        for (const std::string &col_name : parser.get_col_names())
                            table->index_column(col_name);
                }
                else if (main_token == INFO)
                {
                    TableInfoParser parser(input);
                    parser.parse();
                    tableptr table = this->get_table(parser.get_target_name());
                    std::vector<std::pair<std::string, Column>> table_columns = table->get_columns();
                    std::cout << "Table name: " << parser.get_target_name() << std::endl
                              << std::endl;
                    ;
                    for (const std::pair<std::string, Column> &column : table_columns)
                    {
                        std::cout << "Column    : " << column.first << std::endl;
                        std::cout << "Type      : " << column.second.type << std::endl;
                        std::cout << "Width     : " << column.second.width << std::endl;
                        std::cout << std::endl;
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
        this->cleanup();
    }

    void Driver::cleanup()
    {
        this->tables.clear();
        delete this->db;
        this->db = nullptr;
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
void print_vv(const std::vector<std::vector<std::string>> &vv)
{
    for (const std::vector<std::string> &v : vv)
    {
        for (const std::string &s : v)
        {
            std::cout << s << "|";
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}
void to_lower_case(std::string &str)
{
    for (size_t i = 0; i < str.length(); i++)
    {
        str[i] = std::tolower(str[i]);
    }
}
