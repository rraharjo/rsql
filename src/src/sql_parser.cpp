#include "sql_parser.h"
#include <stdexcept>

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

    DeleteParser::DeleteParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    DeleteParser::~DeleteParser()
    {
    }
    void DeleteParser::parse()
    {
    }

    SearchParser::SearchParser(const std::string instruction) : SQLParser(instruction)
    {
    }
    SearchParser::~SearchParser()
    {
    }
    void SearchParser::parse()
    {
    }
}