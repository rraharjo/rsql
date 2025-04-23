#ifndef RSQL_COND
#define RSQL_COND
#include <vector>
#include "data_type.h"
namespace rsql{
    class SingleCondition{
        char *src;
        Cell target;
    };
    class Condition{
        private:
            // this_cond
            std::vector<Condition *> conds;
        protected:
            Condition();
        public:
            bool valid(const char *src);
    };
    class OrCondition : public Condition{

    };
    class AndCondition : public Condition{

    };
}
#endif