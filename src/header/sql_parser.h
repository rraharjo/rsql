#ifndef SQL_PARSER_H
#define SQL_PARSER_H
#include <vector>
#include <string>
#define INSERT "insert"
#define DELETE "delete"
#define SELECT "select"
#define FROM "from"
#define INTO "into"
#define VALUES "values"

namespace rsql
{
    class SQLParser
    {
    protected:
        std::string instruction;
        std::string table_name;
        size_t processed_char_idx;
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
        std::vector<std::vector<std::string>> &get_rows();
    };

    class DeleteParser : public SQLParser
    {
    public:
        void parse() override;
        DeleteParser(const std::string instruction);
        ~DeleteParser();
    };

    class SearchParser : public SQLParser
    {
    public:
        void parse() override;
        SearchParser(const std::string instruction);
        ~SearchParser();
    };
}
#endif