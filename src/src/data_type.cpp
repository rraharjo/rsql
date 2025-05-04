#include "data_type.h"

rsql::DataType rsql::str_to_dt(std::string string_repr)
{
    if (string_repr.compare(DEF_KEY_STR) == 0)
    {
        return rsql::DataType::DEFAULT_KEY;
    }
    else if (string_repr.compare(UINT_STR) == 0)
    {
        return rsql::DataType::UINT;
    }
    else if (string_repr.compare(SINT_STR) == 0)
    {
        return rsql::DataType::SINT;
    }
    else if (string_repr.compare(CHAR_STR) == 0)
    {
        return rsql::DataType::CHAR;
    }
    else if (string_repr.compare(DATE_STR) == 0)
    {
        return rsql::DataType::DATE;
    }
    else
    {
        throw std::invalid_argument(string_repr + " does not represent any data type");
    }
}
std::string rsql::dt_to_str(rsql::DataType type)
{
    switch (type)
    {
    case rsql::DataType::DEFAULT_KEY:
        return DEF_KEY_STR;
    case rsql::DataType::UINT:
        return UINT_STR;
    case rsql::DataType::SINT:
        return SINT_STR;
    case rsql::DataType::CHAR:
        return CHAR_STR;
    case rsql::DataType::DATE:
        return DATE_STR;
    default:
        throw std::invalid_argument("Unknown type");
    }
}
bool rsql::valid_date(const std::string &date)
{
    if (date.size() != 10)
    {
        return false;
    }
    if (date[4] != '-' || date[7] != '-')
    {
        return false;
    }
    try
    {
        int year = std::stoi(date.substr(0, 4));
        int month = std::stoi(date.substr(5, 2));
        int day = std::stoi(date.substr(8, 2));
        if (month < 1 || month > 12)
        {
            return false;
        }
        if (day < 1 || day > 31)
        {
            return false;
        }
        if ((month == 4 || month == 6 || month == 9 || month == 11) && day == 31)
        {
            return false;
        }
        if (month == 2)
        {
            if (day > 29)
            {
                return false;
            }
            if (day == 29 && (year % 4 != 0 || (year % 100 == 0 && year % 400 != 0)))
            {
                return false;
            }
        }
    }
    catch (const std::invalid_argument &e)
    {
        return false;
    }
    catch (const std::out_of_range &e)
    {
        return false;
    }
    return true;
}
void rsql::increment_default_key(unsigned char *const key){
    unsigned int cur_idx = DEFAULT_KEY_WIDTH;
    bool go_left = true;
    while (go_left){
        cur_idx--;
        key[cur_idx]++;
        if (key[cur_idx] != 0){
            go_left = false;
        }
    }
}

namespace rsql
{
    void inline validate_cells(const Cell *c1, const Cell *c2)
    {
        if (c1->type != c2->type)
        {
            throw std::invalid_argument("Can't compare cell with different data type");
        }
    }
    Cell::Cell(const size_t len, const DataType type) : len(len), type(type)
    {
    }
    bool Cell::operator==(const Cell &other) const
    {
        validate_cells(this, &other);
        switch (this->type)
        {
        case DataType::DEFAULT_KEY:
        case DataType::CHAR:
        case DataType::DATE:
            return strncmp(this->src, other.src, this->len) == 0;
            break;
        case DataType::UINT:
        {
            boost::multiprecision::cpp_int this_int, other_int;
            boost::multiprecision::import_bits(this_int, this->src, this->src + this->len, 8, false);
            boost::multiprecision::import_bits(other_int, other.src, other.src + other.len, 8, false);
            return this_int == other_int;
        }
        default:
            throw std::invalid_argument("Can't compare unknown data type");
            return false;
        }
    }
    bool Cell::operator!=(const Cell &other) const
    {
        return !(*this == other);
    }
    bool Cell::operator<(const Cell &other) const
    {
        validate_cells(this, &other);
        switch (this->type)
        {
        case DataType::DEFAULT_KEY:
        case DataType::CHAR:
        case DataType::DATE:
            return strncmp(this->src, other.src, this->len) < 0;
            break;
        case DataType::UINT:
        {
            boost::multiprecision::cpp_int this_int, other_int;
            boost::multiprecision::import_bits(this_int, this->src, this->src + this->len, 8, false);
            boost::multiprecision::import_bits(other_int, other.src, other.src + other.len, 8, false);
            return this_int < other_int;
        }
        default:
            throw std::invalid_argument("Can't compare unknown data type");
            return false;
        }
    }
    bool Cell::operator<=(const Cell &other) const
    {
        validate_cells(this, &other);
        switch (this->type)
        {
        case DataType::DEFAULT_KEY:
        case DataType::CHAR:
        case DataType::DATE:
            return strncmp(this->src, other.src, this->len) <= 0;
            break;
        case DataType::UINT:
        {
            boost::multiprecision::cpp_int this_int, other_int;
            boost::multiprecision::import_bits(this_int, this->src, this->src + this->len, 8, false);
            boost::multiprecision::import_bits(other_int, other.src, other.src + other.len, 8, false);
            return this_int <= other_int;
        }
        default:
            throw std::invalid_argument("Can't compare unknown data type");
            return false;
        }
    }
    bool Cell::operator>(const Cell &other) const
    {
        return !(*this <= other);
    }
    bool Cell::operator>=(const Cell &other) const
    {
        return !(*this < other);
    }

    ConstCell::ConstCell(const char *src, const size_t len, const DataType type) : Cell(len, type)
    {
        this->src = new char[this->len];
        std::memcpy(this->src, src, this->len);
    }

    ColumnCell::ColumnCell(char *src, const size_t len, const DataType type) : Cell(len, type)
    {
        this->src = src;
    }
}