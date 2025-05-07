#ifndef COMPARISON_H
#define COMPARISON_H
#include <vector>
#include "data_type.h"
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

    class Comparison
    {
    protected:
        Comparison();
        Comparison(const Comparison &other);

    public:
        virtual ~Comparison();
        virtual Comparison *clone() = 0;
        virtual bool compare(const char *const row) = 0;
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
         * @param right_val the value of the constant
         */
        ConstantComparison(DataType type, CompSymbol symbol, size_t len, const size_t left_preceding, const char *right_val);
        ConstantComparison(const ConstantComparison &other);
        ~ConstantComparison();

        ConstantComparison *clone() override;
        bool compare(const char *const row) override;
    };

    class MultiComparisons : public Comparison
    {
    protected:
        std::vector<Comparison *> conditions;
        MultiComparisons();
        MultiComparisons(const MultiComparisons &other);

    public:
        virtual ~MultiComparisons();
        /**
         * @brief Add condition to the multicomparison. Does not take ownership of the pointer
         * 
         * @param comparison 
         */
        void add_condition(Comparison *comparison);
    };

    class ORComparisons : public MultiComparisons
    {
    public:
        ORComparisons();
        ORComparisons(const ORComparisons &other);
        ~ORComparisons();
        ORComparisons *clone() override;
        bool compare(const char *const row) override;
    };

    class ANDComparisons : public MultiComparisons
    {
    public:
        ANDComparisons();
        ANDComparisons(const ANDComparisons &other);
        ~ANDComparisons();
        ANDComparisons *clone() override;
        bool compare(const char *const row) override;
    };
}
#endif