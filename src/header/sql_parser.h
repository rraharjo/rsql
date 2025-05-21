#ifndef SQL_PARSER_H
#define SQL_PARSER_H
#include <vector>
#include <string>
#include "comparison.h"
#include "table.h"
#define INSERT "insert"
#define DELETE "delete"
#define SELECT "select"
#define FROM "from"
#define INTO "into"
#define VALUES "values"
#define WHERE "where"
#define AND "and"
#define OR "or"

namespace rsql
{
    class SQLParser
    {
    protected:
        std::string instruction;
        std::string table_name;
        size_t cur_idx;
        SQLParser(const std::string instruction);

    public:
        virtual ~SQLParser();
        virtual void parse() = 0;

        static bool parse_driver();

        inline std::string get_table_name()
        {
            return this->table_name;
        }
        
        void expect(const std::string token);
        std::string extract_next();
        /**
         * @brief Find out what the next token is without extracting it (the index does not change)
         * 
         * @return std::string 
         */
        std::string next_token();
    };

    class ParserWithWhere : public SQLParser {
        protected:
            Comparison *comparison;
            ParserWithWhere(const std::string instruction);
        public:
            virtual ~ParserWithWhere();
            void extract_conditions(Table *table);
    };

    class InsertParser : public SQLParser
    {
    private:
        void extract_values();

    public:
        std::vector<std::vector<std::string>> row_values;
        void parse() override;
        InsertParser(const std::string instruction);
        ~InsertParser();
    };

    class DeleteParser : public ParserWithWhere
    {
    public:
        void parse() override;
        DeleteParser(const std::string instruction);
        ~DeleteParser();
    };

    class SearchParser : public ParserWithWhere
    {
    public:
        void parse() override;
        SearchParser(const std::string instruction);
        ~SearchParser();
    };
}
#endif