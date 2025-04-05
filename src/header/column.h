#ifndef COLUMN_H
#define COLUMN_H
#include <string>
#include "data_type.h"

#define MAX_COL_NAME 64
#define DATE_COL_W 10
#define PKEY_COL_W 32 //32 bytes -> 248 bits pretty -> 2^248 pretty sure this is big enough :)
namespace rsql
{
    class Column
    {
    public:
        // std::string col_name;
        // How many bytes the column is
        size_t width;
        DataType type;
        Column(size_t width, DataType type);

        static Column pkey_column();
        static Column int_column(size_t width);
        static Column char_column(size_t width);
        static Column date_column();

        static Column get_column(DataType type, size_t width);
    };
}
#endif