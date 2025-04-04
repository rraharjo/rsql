#include "column.h"

namespace rsql
{
    Column::Column(size_t width) : width(width)
    {
    }

    size_t Column::get_width()
    {
        return this->width;
    }

    IntColumn::IntColumn(size_t width) : Column(width){}
    DateColumn::DateColumn() : Column(DATE_COL_W){}
    CharColumn::CharColumn(size_t width) : Column(width){}
}