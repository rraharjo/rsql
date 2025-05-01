#include "column.h"
size_t round_up_int_width(const size_t width);
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
            if (!valid_date(src))
            {
                std::string err_msg = src + " is not a valid date";
                throw std::invalid_argument(err_msg);
            }
            std::memset(dest, 0, this->width);
            std::memcpy(dest, src.c_str(), src.length());
            break;
        case DataType::CHAR:
        case DataType::PKEY:
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
    int Column::compare_key(const char *const k1, const char *const k2)
    {
        switch (this->type)
        {
        case DataType::PKEY:
        case DataType::DATE:
        case DataType::CHAR:
        {
            int cmp = std::strncmp(k1, k2, this->width);
            if (cmp < 0)
            {
                return -1;
            }
            else if (cmp > 0)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
        case DataType::UINT:
        {
            boost::multiprecision::cpp_int c_int1, c_int2;
            boost::multiprecision::import_bits(c_int1, k1, k1 + this->width, 8, false);
            boost::multiprecision::import_bits(c_int2, k2, k2 + this->width, 8, false);
            if (c_int1 < c_int2)
            {
                return -1;
            }
            else if (c_int1 > c_int2)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
        case DataType::SINT:
        {
            signed char char_sign1, char_sign2;
            char_sign1 = *k1;
            char_sign2 = *k2;
            int sign1 = static_cast<int>(char_sign1), sign2 = static_cast<int>(char_sign2);
            boost::multiprecision::cpp_int c_int1, c_int2;
            boost::multiprecision::import_bits(c_int1, k1 + 1, k1 + this->width, 8, false);
            boost::multiprecision::import_bits(c_int2, k2 + 1, k2 + this->width, 8, false);
            c_int1 *= sign1;
            c_int2 *= sign2;
            if (c_int1 < c_int2)
            {
                return -1;
            }
            else if (c_int1 > c_int2)
            {
                return 1;
            }
            else
            {
                return 0;
            }
        }
        default:
            throw std::invalid_argument("Unknown DataType");
        }
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
        return Column(col_id, PKEY_COL_W, DataType::PKEY);
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
        case DataType::PKEY:
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

inline size_t round_up_int_width(const size_t width){
    for (size_t i = 1 ; i <= 128 ; i *= 2){
        if (width <= i){
            return i;
        }
    }
    throw std::invalid_argument("Can't create an larger than 128 bytes");
    return 0;
}