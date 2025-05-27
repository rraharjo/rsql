#ifndef COMPARISON_H
#define COMPARISON_H
#include <vector>
#include "data_type.h"
#define COL_COMP_TYPE 1
#define CONST_COMP_TYPE 2
#define AND_COMP_TYPE 3
#define OR_COMP_TYPE 4
namespace rsql
{
    enum class CompSymbol
    {
        EQ,
        GT,
        LT,
        GEQ,
        LEQ
    };

    CompSymbol get_symbol_from_string(const std::string src);

    class Comparison
    {
    protected:
        Comparison();
        Comparison(const Comparison &other);

    public:
        virtual ~Comparison();
        virtual Comparison *clone() = 0;
        virtual bool compare(const char *const row) = 0;
        virtual uint8_t get_comparison_type() const = 0;
        virtual bool operator==(const Comparison &other) const = 0;
        inline bool operator!=(const Comparison &other)
        {
            return !(*this == other);
        }
    };

    class SingleComparison : public Comparison
    {
    protected:
        const DataType type;
        const CompSymbol symbol;
        const size_t left_preceding;
        const size_t left_len;
        SingleComparison(const DataType type, const CompSymbol symbol, const size_t left_preceding, const size_t len);
        SingleComparison(const SingleComparison &other);

    public:
        virtual bool operator==(const Comparison &other) const override;
        virtual ~SingleComparison();
    };

    class ColumnComparison : public SingleComparison
    {
    protected:
        const size_t right_len;
        const size_t right_preceding;

    public:
        /**
         * @brief Construct a new Column Comparison object. Compare left to right
         *
         * @param type DataType being compared
         * @param symbol <, <=, ==, >=, >
         * @param left_len the length of the left column
         * @param left_preceding the preceding length of the left column
         * @param right_preceding the preceding length of the right column
         * @param right_len the length of the right column
         */
        ColumnComparison(DataType type, CompSymbol symbol, size_t left_len, const size_t left_preceding, const size_t right_len, const size_t right_preceding);
        ColumnComparison(const ColumnComparison &other);
        ~ColumnComparison();

        ColumnComparison *clone() override;
        bool compare(const char *const row) override;
        inline uint8_t get_comparison_type() const override
        {
            return COL_COMP_TYPE;
        };
        bool operator==(const Comparison &other) const override;
    };

    class ConstantComparison : public SingleComparison
    {
    protected:
        char *constant_val;

    public:
        /**
         * @brief Construct a new Constant Comparison object. compare left to constant. Does not take ownership of right_val
         *
         * @param type data type being compared
         * @param symbol <, <=, ==, >=, >
         * @param len length of the data being compared
         * @param left_preceding the preceding size of the left column
         * @param right_val the value of the constant. If right_val is nullptr, the value will not be initialized
         */
        ConstantComparison(DataType type, CompSymbol symbol, size_t len, const size_t left_preceding, const char *right_val);
        ConstantComparison(const ConstantComparison &other);
        ~ConstantComparison();

        ConstantComparison *clone() override;
        bool compare(const char *const row) override;
        inline uint8_t get_comparison_type() const override
        {
            return CONST_COMP_TYPE;
        };
        bool operator==(const Comparison &other) const override;
        void change_right_val(const char *new_right_val);
    };

    class MultiComparisons : public Comparison
    {
    protected:
        std::vector<Comparison *> conditions;
        MultiComparisons();
        MultiComparisons(const MultiComparisons &other);

    public:
        virtual ~MultiComparisons();
        virtual bool operator==(const Comparison &other) const override;
        /**
         * @brief Add condition to the multicomparison. Does not take ownership of the pointer
         *
         * @param comparison
         */
        void add_condition(Comparison *comparison);
        virtual bool is_and() = 0;
    };

    class ORComparisons : public MultiComparisons
    {
    public:
        ORComparisons();
        ORComparisons(const ORComparisons &other);
        ~ORComparisons();
        ORComparisons *clone() override;
        bool compare(const char *const row) override;
        uint8_t get_comparison_type() const override
        {
            return OR_COMP_TYPE;
        };
        bool is_and() override;
    };

    class ANDComparisons : public MultiComparisons
    {
    public:
        ANDComparisons();
        ANDComparisons(const ANDComparisons &other);
        ~ANDComparisons();
        ANDComparisons *clone() override;
        bool compare(const char *const row) override;
        uint8_t get_comparison_type() const override
        {
            return AND_COMP_TYPE;
        };
        bool is_and() override;
    };
}
#endif