#ifndef COLUMN_H
#define COLUMN_H
#include <string>

#define MAX_COL_NAME 64
#define DATE_COL_W 10
namespace rsql{
    class Column{
        protected:
            //std::string col_name;
            // How many bytes the column is
            size_t width;
        public:
            Column(size_t width);

            size_t get_width();
    };

    class IntColumn : public Column{
        public:
            IntColumn(size_t width);
    };

    class DateColumn: public Column{
        public:
            //dd-MM-yyyy
            DateColumn();
    };

    class CharColumn : public Column{
        public:
            CharColumn(size_t width);
    };
}
#endif