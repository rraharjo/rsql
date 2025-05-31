#include "column.h"
static bool valid_numeric(const std::string &str, bool with_sign = true)
{
    if (str.length() == 0)
        return true;
    bool is_negative = str[0] == '-' ? true : false;
    if (!with_sign && is_negative)
        return false;
    bool valid_number = true;
    size_t i = 0;
    if (is_negative)
        i = 1;
    while (i < str.length())
    {
        valid_number = valid_number && str[i] >= '0' && str[i] <= '9';
        i++;
    }
    return valid_number;
}

namespace rsql
{
    Column::Column(unsigned int col_id, size_t width, DataType type) : col_id(col_id), width(width), type(type)
    {
    }

    void Column::process_string(char *const dest, const std::string src)
    {
        switch (this->type)
        {
        case DataType::DATE:
        {
            if (src.length() > 1 && (src[0] != '"' || src[src.length() - 1] != '"'))
            {
                std::string err_msg = src + " is not a valid date";
                throw std::invalid_argument(err_msg);
            }
            std::string stripped = src.substr(1, src.length() - 2);
            if (!valid_date(stripped))
            {
                std::string err_msg = stripped + " is not a valid date";
                throw std::invalid_argument(err_msg);
            }
            std::memset(dest, 0, this->width);
            std::memcpy(dest, stripped.c_str(), stripped.length());
            break;
        }
        case DataType::CHAR:
        {
            if (src.length() > 1 && (src[0] != '"' || src[src.length() - 1] != '"'))
            {
                std::string err_msg = src + " is not a valid string";
                throw std::invalid_argument(err_msg);
            }
            std::string stripped = src.substr(1, src.length() - 2);
            if (stripped.length() > this->width)
            {
                throw std::invalid_argument("byte overflow");
                return;
            }
            std::memset(dest, 0, this->width);
            std::memcpy(dest, stripped.c_str(), stripped.length());
            break;
        }
        case DataType::DEFAULT_KEY:
            if (src.length() > this->width)
            {
                throw std::invalid_argument("byte overflow");
                return;
            }
            std::memset(dest, 0, this->width);
            std::memcpy(dest, src.c_str(), src.length());
            break;
        case DataType::UINT:
        {
            if (!valid_numeric(src, false))
                throw std::invalid_argument("Invalid unsigned number " + src);
            boost::multiprecision::cpp_int int_val(src);
            std::vector<unsigned char> buff;
            std::memset(dest, 0, this->width);
            export_bits(int_val, std::back_inserter(buff), 8, false);
            if (buff.size() > this->width)
            {
                throw std::invalid_argument("Byte overflow");
                return;
            }
            std::memcpy(dest, buff.data(), buff.size());
            break;
        }
        case DataType::SINT:
        {
            if (!valid_numeric(src))
                throw std::invalid_argument("Invalid number " + src);
            boost::multiprecision::cpp_int magnitude(src);
            int8_t sign = (int8_t)magnitude.sign();
            std::vector<unsigned char> buff;
            std::memset(dest, 0, this->width);
            buff.push_back((unsigned char)sign);
            export_bits(magnitude, std::back_inserter(buff), 8, false);
            if (buff.size() > this->width)
            {
                throw std::invalid_argument("Byte overflow");
                return;
            }
            std::memcpy(dest, buff.data(), buff.size());
            break;
        }
        }
    }
    std::string Column::process_stream(const char *const src)
    {
        switch (this->type)
        {
        case DataType::DEFAULT_KEY:
        {
            boost::multiprecision::cpp_int dkey_val;
            boost::multiprecision::import_bits(dkey_val, src, src + DEFAULT_KEY_WIDTH, 8, true);
            return dkey_val.str();
        }
        case DataType::CHAR:
        case DataType::DATE:
        {
            std::string to_ret("", this->width);
            std::memcpy(to_ret.data(), src, this->width);
            if (to_ret[to_ret.length() - 1] == '\0')
                to_ret.resize(std::strlen(to_ret.data()));
            return to_ret;
        }
        case DataType::UINT:
        {
            boost::multiprecision::cpp_int temp;
            char_to_ucpp_int((char *const)src, this->width, temp);
            return temp.str();
        }
        case DataType::SINT:
        {
            boost::multiprecision::cpp_int temp;
            char_to_scpp_int((char *const)src, this->width, temp);
            return temp.str();
        }
        default:
            throw std::invalid_argument("Unknown data type");
        }
    }
    int Column::compare_key(const char *const k1, const char *const k2, CompSymbol symbol)
    {
        ConstantComparison c(this->type, symbol, this->width, 0, k2);
        return c.compare(k1);
    }

    bool Column::operator==(const Column &other) const
    {
        return this->col_id == other.col_id && this->type == other.type && this->width == other.width;
    }

    bool Column::operator!=(const Column &other) const
    {
        return !(*this == other);
    }

    Column Column::pkey_column(unsigned int col_id)
    {
        return Column(col_id, DEFAULT_KEY_WIDTH, DataType::DEFAULT_KEY);
    }
    Column Column::unsigned_int_column(unsigned int col_id, const size_t width)
    {
        return Column(col_id, width, DataType::UINT);
    }
    Column Column::signed_int_column(unsigned int col_id, const size_t width)
    {
        if (width <= 1)
        {
            throw std::invalid_argument("Can't create a signed column less than 1 byte");
        }
        return Column(col_id, width, DataType::SINT);
    }
    Column Column::char_column(unsigned int col_id, const size_t width)
    {
        return Column(col_id, width, DataType::CHAR);
    }
    Column Column::date_column(unsigned int col_id)
    {
        return Column(col_id, DATE_COL_W, DataType::DATE);
    }
    Column Column::get_column(unsigned int col_id, DataType type, size_t width)
    {
        switch (type)
        {
        case DataType::DEFAULT_KEY:
            return Column::pkey_column(col_id);
        case DataType::UINT:
            return Column::unsigned_int_column(col_id, width);
        case DataType::SINT:
            return Column::signed_int_column(col_id, width);
        case DataType::CHAR:
            return Column::char_column(col_id, width);
        case DataType::DATE:
            return Column::date_column(col_id);
        default:
            throw std::invalid_argument("Unknown DataType");
        }
    }
}