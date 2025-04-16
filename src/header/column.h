#ifndef COLUMN_H
#define COLUMN_H
#include <string>
#include "data_type.h"

#define COLUMN_BYTES 12
#define MAX_COL_NAME 64
#define DATE_COL_W 10
#define PKEY_COL_W 32 //32 bytes -> 248 bits pretty -> 2^248 pretty sure this is big enough :)
namespace rsql
{
    class Column
    {
    public:
        unsigned int col_id;
        size_t width;
        DataType type;
        
        Column(unsigned int col_id, size_t width, DataType type);
        bool operator==(const Column &other) const;
        bool operator!=(const Column &other) const;

        static Column pkey_column(unsigned int col_id);
        static Column int_column(unsigned int col_id, size_t width);
        static Column char_column(unsigned int col_id, size_t width);
        static Column date_column(unsigned int col_id);

        static Column get_column(unsigned int col_id, DataType type, size_t width);
    };
}
#endif