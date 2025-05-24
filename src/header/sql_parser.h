#ifndef SQL_PARSER_H
#define SQL_PARSER_H
#include <vector>
#include <string>
#include "comparison.h"
#include "table.h"
#define CREATE "create"
#define CONNECT "connect"
#define INSERT "insert"
#define DELETE "delete"
#define SELECT "select"
#define FROM "from"
#define INTO "into"
#define VALUES "values"
#define WHERE "where"
#define AND "and"
#define OR "or"
#define DATABASE "database"
#define TABLE "table"

namespace rsql
{
    class Driver;
    class SQLParser
    {
    protected:
        std::string instruction;
        std::string target_name;
        size_t cur_idx;
        SQLParser(const std::string instruction);

    public:
        virtual ~SQLParser();
        virtual void parse() = 0;

        static bool parse_driver();

        inline std::string get_target_name()
        {
            return this->target_name;
        }

        void expect(const std::string token);
        /**
         * @brief extract the next token (index changes)
         *
         * @return std::string
         */
        std::string extract_next();
        /**
         * @brief Find out what the next token is without extracting it (the index does not change)
         *
         * @return std::string
         */
        std::string next_token();
    };

    class ParserWithWhere : public SQLParser
    {
    protected:
        std::string main_col_name = DEF_KEY_COL_NAME;
        rsql::CompSymbol main_symbol = rsql::CompSymbol::EQ;
        char *main_val = nullptr;
        Comparison *comparison;
        ParserWithWhere(const std::string instruction);

    public:
        virtual ~ParserWithWhere();
        void extract_conditions(Table *table);
        friend class Driver;
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

    class CreateDBParser : public SQLParser
    {
    public:
        CreateDBParser(const std::string instruction);
        ~CreateDBParser();
        void parse() override;
    };

    class CreateTableParser : public SQLParser
    {
    public:
        std::vector<std::pair<std::string, Column>> columns;
        CreateTableParser(const std::string instruction);
        ~CreateTableParser();
        void parse() override;
        void parse_columns();
    };

    class ConnectParser : public SQLParser
    {
    public:
        ConnectParser(const std::string instruction);
        ~ConnectParser();
        void parse() override;
    };
}
#endif