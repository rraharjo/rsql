#include "column.h"
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
            if (src.length() > this->width){
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
            if (buff.size() > this->width){
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
            if (buff.size() > this->width){
                throw std::invalid_argument("Byte overflow");
                return;
            }
            std::memcpy(dest, buff.data(), buff.size());
            break;
        }
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
        if (width <= 1){
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