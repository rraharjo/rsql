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
#define ALTER "alter"
#define FROM "from"
#define INTO "into"
#define VALUES "values"
#define WHERE "where"
#define AND "and"
#define OR "or"
#define ADD "add"
#define INDEX_COL "index"
#define DATABASE "database"
#define TABLE "table"
#define INFO "info"
#define LIST_DB "list"

namespace rsql
{
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

    class ParserWithComparison : public SQLParser
    {
    protected:
        std::string main_col_name = DEF_KEY_COL_NAME;
        rsql::CompSymbol main_symbol = rsql::CompSymbol::EQ;
        char *main_val = nullptr;
        Comparison *comparison;
        ParserWithComparison(const std::string instruction);

    public:
        virtual ~ParserWithComparison();
        void extract_comparisons(Table *table);
        inline const Comparison *get_comparison() const { return this->comparison; }
        inline const std::string &get_main_col_name() const { return this->main_col_name; }
        inline const rsql::CompSymbol &get_main_symbol() const { return this->main_symbol; }
        inline const char *get_main_val() const { return this->main_val; }

        friend class Driver;
    };

    class ParserWithColumns : public SQLParser
    {
    protected:
        std::vector<std::pair<std::string, Column>> new_columns;
        ParserWithColumns(const std::string instruction);
        virtual ~ParserWithColumns();

    public:
        void parse_columns();
        inline const std::vector<std::pair<std::string, Column>> get_columns() const { return this->new_columns; }
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

    class DeleteParser : public ParserWithComparison
    {
    public:
        void parse() override;
        DeleteParser(const std::string instruction);
        ~DeleteParser();
    };

    class SearchParser : public ParserWithComparison
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

    class CreateTableParser : public ParserWithColumns
    {
    public:
        CreateTableParser(const std::string instruction);
        ~CreateTableParser();
        void parse() override;
    };

    class AlterTableParser : public ParserWithColumns
    {
    private:
        std::vector<std::string> col_names;
    public:
        AlterTableParser(const std::string instruction);
        ~AlterTableParser();
        void parse() override;
        inline const std::vector<std::string> &get_col_names() const { return this->col_names; }
    };

    class ConnectParser : public SQLParser
    {
    public:
        ConnectParser(const std::string instruction);
        ~ConnectParser();
        void parse() override;
    };

    class TableInfoParser : public SQLParser
    {
    public:
        TableInfoParser(const std::string instruction);
        ~TableInfoParser();
        void parse() override;
    };

    class DatabaseInfoParser : public SQLParser
    {
    public:
        DatabaseInfoParser(const std::string instruction);
        ~DatabaseInfoParser();
        void parse() override;
    };
}
#endif