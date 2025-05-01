//
// Created by zhengjin.wang on 25-4-30.
//

#ifndef ASSERT_H
#define ASSERT_H
#define Fail(msg)   \
    std::cout << (std::string("Invalid input error: ") + msg);


#define Assert(expr, msg)         \
if (!static_cast<bool>(expr)) { \
Fail(msg);                    \
}                               \
static_assert(true, "End call of macro with a semicolon.")
#endif //ASSERT_H
