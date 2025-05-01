#ifndef COMPARISON_H
#define COMPARISON_H
#include <vector>
#include "data_type.h"
namespace rsql
{
    //TODO: create copy constructor
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
        const CompSymbol symbol;
        const DataType type;
        const size_t len;
        SingleComparison(DataType type, CompSymbol symbol, size_t len);
        SingleComparison(const SingleComparison &other);
    public:
        virtual ~SingleComparison();
    };

    class ColumnComparison : public SingleComparison
    {
    protected:
        const size_t left_preceding;
        const size_t right_preceding;
        const size_t right_len;

    public:
        ColumnComparison(DataType type, CompSymbol symbol, size_t left_len, const size_t left_preceding, const size_t right_preceding, const size_t right_len);
        ColumnComparison(const ColumnComparison &other);
        ~ColumnComparison();

        ColumnComparison *clone() override;
        bool compare(const char *const row) override;
    };

    class ConstantComparison : public SingleComparison
    {
    protected:
        const size_t preceding_size;
        char *constant_val;

    public:
        ConstantComparison(DataType type, CompSymbol symbol, size_t len, const size_t col_preceding, const char *right);
        ConstantComparison(const ConstantComparison &other);
        ~ConstantComparison();

        ConstantComparison *clone() override;
        bool compare(const char *const row) override;
    };

    class MultiComparisons : public Comparison{
        protected:
            std::vector<Comparison *> conditions;
            MultiComparisons();
            MultiComparisons(const MultiComparisons &other);
        public:
            virtual ~MultiComparisons();
            void add_condition(Comparison *comparison);
    };

    class ORComparisons : public MultiComparisons{
        public:
            ORComparisons();
            ORComparisons(const ORComparisons &other);
            ~ORComparisons();
            ORComparisons *clone() override;
            bool compare(const char *const row) override;
    };

    class ANDComparisons : public MultiComparisons{
        public:
            ANDComparisons();
            ANDComparisons(const ANDComparisons &other);
            ~ANDComparisons();
            ANDComparisons *clone() override;
            bool compare(const char *const row) override;
    };
}
#endif