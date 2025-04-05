#include "column.h"
namespace rsql
{
    Column::Column(size_t width, DataType type) : width(width), type(type)
    {
    }

    Column Column::pkey_column()
    {
        return Column(PKEY_COL_W, DataType::PKEY);
    }
    Column Column::int_column(size_t width)
    {
        return Column(width, DataType::INT);
    }
    Column Column::char_column(size_t width)
    {
        return Column(width, DataType::CHAR);
    }
    Column Column::date_column()
    {
        return Column(DATE_COL_W, DataType::DATE);
    }
    Column Column::get_column(DataType type, size_t width)
    {
        if (type == DataType::PKEY)
        {
            return Column::pkey_column();
        }
        else if (type == DataType::INT)
        {
            return Column::int_column(width);
        }
        else if (type == DataType::CHAR)
        {
            return Column::char_column(width);
        }
        else if (type == DataType::DATE)
        {
            return Column::date_column();
        }
        else
        {
            throw std::invalid_argument("Unknown DataType");
        }
    }
}