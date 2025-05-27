#include "sql_parser.h"
#include <stdexcept>
#include <memory>

static std::vector<std::string> tokenize_sp_and_parenthesis(std::string str)
{
    size_t l = -1;
    size_t r = 0;
    std::vector<std::string> to_ret;
    while (r < str.length())
    {
        unsigned char cur_char = str[r];
        if (cur_char == ' ')
        {
            if (l + 1 != r)
            {
                size_t len = r - (l + 1);
                to_ret.push_back(str.substr(l + 1, len));
            }
            l = r;
        }
        else if (cur_char == '(' || cur_char == ')')
        {
            if (l + 1 != r)
            {
                size_t len = r - (l + 1);
                to_ret.push_back(str.substr(l + 1, len));
            }
            to_ret.push_back(str.substr(r, 1));
            l = r;
        }
        r++;
    }
    return to_ret;
}

static std::string strip(std::string str, unsigned char token = ' ')
{
    size_t left = 0;
    size_t right = str.length() - 1;
    while (left < str.length() && str[left] == token)
    {
        left++;
    }
    while (right > left && str[right] == token)
    {
        right--;
    }
    size_t len = right - left + 1;
    if (len <= 0)
    {
        return "";
    }
    return str.substr(left, len);
}

static rsql::Comparison *get_single_comparison(const std::string l, const std::string sym, const std::string r, rsql::Table *table)
{
    rsql::CompSymbol symbol = rsql::get_symbol_from_string(sym);
    size_t left_preceding = table->get_preceding_length(l);
    rsql::Column col = table->get_column(l);
    std::unique_ptr<char[]> r_val = std::make_unique<char[]>(col.width);
    col.process_string(r_val.get(), r);
    rsql::ConstantComparison *to_ret = new rsql::ConstantComparison(col.type, symbol, col.width, left_preceding, r_val.get());
    return to_ret;
}

static std::string get_error_msg(const std::string &instruction, const size_t idx)
{
    std::string err_msg = "Invalid character at position " + std::to_string(idx) + "\n";
    std::string pointer_buffer(instruction.length(), ' ');
    if (instruction.length() > 0 && idx < instruction.length())
    {
        pointer_buffer[idx] = '^';
    }
    err_msg.append(instruction).push_back('\n');
    err_msg.append(pointer_buffer).push_back('\n');
    return err_msg;
}

static inline void to_lower_case(std::string &str)
{
    for (size_t i = 0; i < str.length(); i++)
    {
        str[i] = std::tolower(str[i]);
    }
}

namespace rsql
{
    SQLParser::SQLParser(const std::string instruction) : instruction(instruction), cur_idx(0)
    {
        // to_lower_case(this->instruction);
    }
    SQLParser::~SQLParser()
    {
    }
    bool SQLParser::parse_driver()
    {
        return false;
    }
    void SQLParser::expect(const std::string token)
    {
        size_t cur_token_size = 0;
        while (this->cur_idx < this->instruction.length() && this->instruction[this->cur_idx] == ' ')
        {
            this->cur_idx++;
        }
        while (cur_token_size < token.length())
        {
            if (this->cur_idx >= this->instruction.length())
            {
                throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                return;
            }
            if (tolower(this->instruction[this->cur_idx]) == tolower(token[cur_token_size]))
            {
                this->cur_idx++;
                cur_token_size++;
            }
            else
            {
                throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                return;
            }
        }
        if (this->cur_idx < this->instruction.length() && this->instruction[this->cur_idx] != ' ')
        {
            throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
            return;
        }
        return;
    }
    std::string SQLParser::extract_next()
    {
        size_t start_point = this->cur_idx;
        while (this->cur_idx < this->instruction.length() && this->instruction[this->cur_idx] == ' ')
        {
            this->cur_idx++;
        }
        while (this->cur_idx < this->instruction.length() && this->instruction[this->cur_idx] != ' ')
        {
            this->cur_idx++;
        }
        std::string to_ret = this->instruction.substr(start_point, this->cur_idx - start_point);
        return strip(to_ret);
    }
    std::string SQLParser::next_token()
    {
        size_t start_point = this->cur_idx;
        size_t moving_point = this->cur_idx;
        while (moving_point < this->instruction.length() && this->instruction[moving_point] == ' ')
        {
            moving_point++;
        }
        while (moving_point < this->instruction.length() && this->instruction[moving_point] != ' ')
        {
            moving_point++;
        }
        std::string to_ret = this->instruction.substr(start_point, moving_point - start_point);
        return strip(to_ret);
    }

    InsertParser::InsertParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    InsertParser::~InsertParser()
    {
    }
    void InsertParser::extract_values()
    {
        std::vector<std::string> temp;
        bool inside_bracket = false;
        bool inside_quote = false;
        bool expect_new_row = true;
        size_t l = -1;
        while (this->cur_idx < this->instruction.length())
        {
            unsigned char cur_char = this->instruction[this->cur_idx];
            if (inside_quote)
            {
                if (cur_char == '"')
                    inside_quote = false;
            }
            else
            {
                if (cur_char == '(')
                {
                    if (inside_bracket || !expect_new_row)
                        throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                    l = this->cur_idx;
                    inside_bracket = true;
                }
                else if (cur_char == ')')
                {
                    if (!inside_bracket)
                        throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                    temp.push_back(strip(strip(this->instruction.substr(l + 1, this->cur_idx - l - 1)), '"'));
                    l = -1;
                    inside_bracket = false;
                    this->row_values.push_back(temp);
                    temp.clear();
                    expect_new_row = false;
                }
                else if (cur_char == ',')
                {
                    if (inside_bracket)
                    {
                        temp.push_back(strip(strip(this->instruction.substr(l + 1, this->cur_idx - l - 1)), '"'));
                        l = this->cur_idx;
                    }
                    else
                    {
                        if (expect_new_row)
                            throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                        expect_new_row = true;
                    }
                }
                else if (cur_char == '"')
                {
                    inside_quote = true;
                }
            }
            this->cur_idx++;
        }
        if (expect_new_row || inside_bracket || inside_quote)
            throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
    }
    void InsertParser::parse()
    {
        this->expect(INSERT);
        this->expect(INTO);
        this->target_name = this->extract_next();
        this->expect(VALUES);
        this->extract_values();
    }

    ParserWithComparison::ParserWithComparison(const std::string instruction) : SQLParser(instruction), comparison(nullptr)
    {
    }
    ParserWithComparison::~ParserWithComparison()
    {
        delete this->comparison;
        delete[] this->main_val;
    }
    void ParserWithComparison::extract_conditions(Table *table)
    {
        if (this->next_token() == "")
            return;
        this->expect(WHERE);
        std::string n_token = this->next_token();
        bool has_primary_comparison = false;
        // extracting primary col
        if (n_token != "" && n_token[0] != '(')
        {
            this->main_col_name = this->extract_next();
            this->main_symbol = get_symbol_from_string(this->extract_next());
            std::string str_val = this->extract_next();
            Column main_column = table->get_column(this->main_col_name);
            this->main_val = new char[main_column.width];
            main_column.process_string(this->main_val, str_val);
            has_primary_comparison = true;
        }

        std::string optional_token = this->next_token();
        to_lower_case(optional_token);
        if (optional_token.length() == 0)
        {
            return;
        }
        else if (has_primary_comparison)
        {
            this->expect(AND);
        }

        std::vector<std::string> conditions = tokenize_sp_and_parenthesis(this->instruction.substr(this->cur_idx));
        std::stack<size_t> top_size;
        std::stack<std::unique_ptr<Comparison>> comp_reserve;
        std::stack<std::string> or_or_and;
        std::vector<std::string> temp;
        for (size_t i = 0; i < conditions.size(); i++)
        {
            std::string condition_lower = conditions[i];
            to_lower_case(condition_lower);
            if (condition_lower == "(")
            {
                top_size.push(0);
                or_or_and.push("");
            }
            else if (condition_lower == ")")
            {
                size_t to_remove = top_size.top();
                top_size.pop();
                std::string comp_type = or_or_and.top();
                or_or_and.pop();
                std::unique_ptr<rsql::MultiComparisons> and_or_comp;
                if (comp_type == OR)
                    and_or_comp.reset(new ORComparisons());
                else
                    and_or_comp.reset(new ANDComparisons());
                while (to_remove > 0)
                {
                    std::unique_ptr<Comparison> cur_top = std::move(comp_reserve.top());
                    comp_reserve.pop();
                    and_or_comp->add_condition(cur_top.get());
                    cur_top.release();
                    to_remove--;
                }
                if (top_size.empty())
                {
                    top_size.push(1);
                }
                else
                {
                    size_t cur_size = top_size.top();
                    cur_size++;
                    top_size.pop();
                    top_size.push(cur_size);
                }
                comp_reserve.push(std::move(and_or_comp));
            }
            else if (condition_lower == OR)
            {
                or_or_and.pop();
                or_or_and.push(OR);
            }
            else if (condition_lower == AND)
            {
                or_or_and.pop();
                or_or_and.push(AND);
            }
            else
            {
                temp.push_back(conditions[i]);
                if (temp.size() == 3)
                {
                    std::unique_ptr<Comparison> to_add;
                    to_add.reset(get_single_comparison(temp[0], temp[1], temp[2], table));
                    comp_reserve.push(std::move(to_add));
                    size_t cur_size = top_size.top();
                    cur_size++;
                    top_size.pop();
                    top_size.push(cur_size);
                    temp.clear();
                }
            }
        }
        this->cur_idx = this->instruction.length() - 1;
        if (temp.size() != 0 || comp_reserve.size() != 1 || or_or_and.size() != 0)
            throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
        this->comparison = comp_reserve.top().get();
        comp_reserve.top().release();
    }

    DeleteParser::DeleteParser(const std::string instruction) : ParserWithComparison(instruction)
    {
    }
    DeleteParser::~DeleteParser()
    {
    }
    void DeleteParser::parse()
    {
        this->expect(DELETE);
        this->expect(FROM);
        this->target_name = this->extract_next();
    }

    SearchParser::SearchParser(const std::string instruction) : ParserWithComparison(instruction)
    {
    }
    SearchParser::~SearchParser()
    {
    }
    void SearchParser::parse()
    {
        this->expect(SELECT);
        this->expect(FROM);
        this->target_name = this->extract_next();
    }

    CreateDBParser::CreateDBParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    CreateDBParser::~CreateDBParser()
    {
    }
    void CreateDBParser::parse()
    {
        this->expect(CREATE);
        this->expect(DATABASE);
        this->target_name = this->extract_next();
    }

    CreateTableParser::CreateTableParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    CreateTableParser::~CreateTableParser()
    {
    }
    void CreateTableParser::parse()
    {
        this->expect(CREATE);
        this->expect(TABLE);
        this->target_name = this->extract_next();
        this->parse_columns();
    }
    void CreateTableParser::parse_columns()
    {
        bool in_parenthesis = false;
        std::vector<std::string> tokens = tokenize_sp_and_parenthesis(this->instruction.substr(this->cur_idx));
        std::vector<std::string> temp;
        for (size_t i = 0; i < tokens.size(); i++)
        {
            if (tokens[i] == "(")
            {
                if (in_parenthesis)
                    throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                in_parenthesis = true;
            }
            else if (tokens[i] == ")")
            {
                if (!in_parenthesis)
                    throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                in_parenthesis = false;
                try
                {
                    std::string col_name = temp[0];
                    std::string type_str = temp[1];
                    DataType type = str_to_dt(type_str);
                    if (type == DataType::DEFAULT_KEY || type == DataType::DATE)
                    {
                        if (temp.size() != 2)
                        {
                            throw std::exception();
                        }
                        Column to_add = Column::get_column(0, type, 0);
                        this->columns.push_back(std::make_pair(col_name, to_add));
                    }
                    else
                    {
                        if (temp.size() != 3)
                        {
                            throw std::exception();
                        }
                        std::string width_str = temp[2];
                        size_t width = std::stoul(width_str);
                        Column to_add = Column::get_column(0, type, width);
                        this->columns.push_back(std::make_pair(col_name, to_add));
                    }
                    temp.clear();
                }
                catch (const std::exception &e)
                {
                    throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
                }
            }
            else
            {
                temp.push_back(tokens[i]);
            }
        }
        if (in_parenthesis || temp.size() != 0)
        {
            throw std::invalid_argument(get_error_msg(this->instruction, this->cur_idx));
        }
    }

    ConnectParser::ConnectParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    ConnectParser::~ConnectParser()
    {
    }
    void ConnectParser::parse()
    {
        this->expect(CONNECT);
        this->expect(DATABASE);
        this->target_name = this->extract_next();
    }

    TableInfoParser::TableInfoParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    TableInfoParser::~TableInfoParser()
    {
    }
    void TableInfoParser::parse()
    {
        this->expect(INFO);
        this->target_name = this->extract_next();
    }
}