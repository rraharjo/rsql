#include "column.h"
size_t round_up_int_width(const size_t width);
namespace rsql
{
    Column::Column(unsigned int col_id, size_t width, DataType type) : col_id(col_id), width(width), type(type)
    {
    }

    bool Column::operator==(const Column& other) const {
        return this->col_id == other.col_id && this->type == other.type && this->width == other.width;
    }

    bool Column::operator!=(const Column& other) const {
        return !(*this == other);
    }

    Column Column::pkey_column(unsigned int col_id)
    {
        return Column(col_id, PKEY_COL_W, DataType::PKEY);
    }
    Column Column::int_column(unsigned int col_id, size_t width)
    {
        return Column(col_id, round_up_int_width(width), DataType::INT);
    }
    Column Column::char_column(unsigned int col_id, size_t width)
    {
        return Column(col_id, width, DataType::CHAR);
    }
    Column Column::date_column(unsigned int col_id)
    {
        return Column(col_id, DATE_COL_W, DataType::DATE);
    }
    Column Column::get_column(unsigned int col_id, DataType type, size_t width)
    {
        if (type == DataType::PKEY)
        {
            return Column::pkey_column(col_id);
        }
        else if (type == DataType::INT)
        {
            return Column::int_column(col_id, width);
        }
        else if (type == DataType::CHAR)
        {
            return Column::char_column(col_id, width);
        }
        else if (type == DataType::DATE)
        {
            return Column::date_column(col_id);
        }
        else
        {
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