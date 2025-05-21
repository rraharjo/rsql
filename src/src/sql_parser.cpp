#include "sql_parser.h"
#include <stdexcept>
#include <memory>

static std::vector<std::string> where_tokenize(std::string str)
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
    char *r_val = new char[col.width];
    if (col.type == rsql::DataType::UINT)
    {
        boost::multiprecision::cpp_int temp(r);
        rsql::ucpp_int_to_char(r_val, col.width, temp);
    }
    else if (col.type == rsql::DataType::SINT)
    {
        boost::multiprecision::cpp_int temp(r);
        rsql::scpp_int_to_char(r_val, col.width, temp);
    }
    else
    {
        std::memcpy(r_val, r.data(), r.length());
    }
    rsql::ConstantComparison *to_ret = new rsql::ConstantComparison(col.type, symbol, col.width, left_preceding, r_val);
    delete[] r_val;
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
        to_lower_case(this->instruction);
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
            if (this->instruction[this->cur_idx] == token[cur_token_size])
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
        while (this->cur_idx < this->instruction.length() && this->instruction[this->cur_idx] == ' ')
        {
            moving_point++;
        }
        while (this->cur_idx < this->instruction.length() && this->instruction[this->cur_idx] != ' ')
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
                    if (inside_bracket)
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
        this->table_name = this->extract_next();
        this->expect(VALUES);
        this->extract_values();
    }

    ParserWithWhere::ParserWithWhere(const std::string instruction) : SQLParser(instruction), comparison(nullptr)
    {
    }
    ParserWithWhere::~ParserWithWhere()
    {
        delete this->comparison;
    }
    void ParserWithWhere::extract_conditions(Table *table)
    {
        this->expect(WHERE);
        std::vector<std::string> conditions = where_tokenize(this->instruction.substr(this->cur_idx));
        std::stack<size_t> top_size;
        std::stack<Comparison *> comp;
        std::stack<std::string> or_or_and;
        std::vector<std::string> temp;
        for (size_t i = 0; i < conditions.size(); i++)
        {
            if (conditions[i] == "(")
            {
                top_size.push(0);
                or_or_and.push("");
            }
            else if (conditions[i] == ")")
            {
                size_t to_remove = top_size.top();
                top_size.pop();
                std::string comp_type = or_or_and.top();
                or_or_and.pop();

                rsql::MultiComparisons *and_or_comp = nullptr;
                if (comp_type == "or")
                    and_or_comp = new ORComparisons();
                if (comp_type == "and")
                    and_or_comp = new ANDComparisons();
                while (to_remove > 0)
                {
                    and_or_comp->add_condition(comp.top());
                    comp.pop();
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
                comp.push(and_or_comp);
            }
            else if (conditions[i] == "or")
            {
                or_or_and.pop();
                or_or_and.push("or");
            }
            else if (conditions[i] == "and")
            {
                or_or_and.pop();
                or_or_and.push("and");
            }
            else
            {
                temp.push_back(conditions[i]);
                if (temp.size() == 3)
                {
                    comp.push(get_single_comparison(temp[0], temp[1], temp[2], table));
                    size_t cur_size = top_size.top();
                    cur_size++;
                    top_size.pop();
                    top_size.push(cur_size);
                    temp.clear();
                }
            }
        }
        this->comparison = comp.top();
    }

    DeleteParser::DeleteParser(const std::string instruction) : ParserWithWhere(instruction)
    {
    }
    DeleteParser::~DeleteParser()
    {
    }
    void DeleteParser::parse()
    {
        this->expect(DELETE);
        this->expect(FROM);
        this->table_name = this->extract_next();
    }

    SearchParser::SearchParser(const std::string instruction) : ParserWithWhere(instruction)
    {
    }
    SearchParser::~SearchParser()
    {
    }
    void SearchParser::parse()
    {
        this->expect(SELECT);
        this->expect(FROM);
        this->table_name = this->extract_next();
    }
}