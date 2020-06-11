#pragma once

#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

#define CONCAT_(x, y) x##y
#define CONCAT(x, y) CONCAT_(x, y)
#define defer(func, var)                                                     \
   void *                                                                    \
       CONCAT(__defer__, __LINE__) __attribute__((__cleanup__(func))) = var; \
   (void)CONCAT(__defer__, __LINE__)

#define MUST_CHECK __attribute__((warn_unused_result))

#define mark_error() push_error_again(__FILE__, __LINE__, __func__)
#define push_error(code, format, ...) push_error_internal( \
    __FILE__, __LINE__, __func__, code, format, ##__VA_ARGS__)

#define assert_no_existing_errors()                                                   \
   if (get_errors_count())                                                            \
   {                                                                                  \
      push_error(EINVAL, "Cannot call %s when there are unnoticed errors", __func__); \
   }                                                                                  \
   (void)1 /* to allow terminating with ; with no warnings*/

struct cancel_defer
{
   void *target;
   void (*action)(void *);
   size_t cancelled;
};

void _try_defer(struct cancel_defer *cd);

#define try_defer(func, var, cancelled_ptr)                                                   \
   struct cancel_defer                                                                        \
       CONCAT(__defer__, __LINE__) __attribute__((__cleanup__(_try_defer))) = {var, func, 0}; \
   cancelled_ptr = &CONCAT(__defer__, __LINE__).cancelled

__attribute__((__format__(__printf__, 5, 6))) void push_error_internal(const char *file, uint32_t line, const char *func, int32_t code, const char *format, ...);

void push_error_again(const char *file, uint32_t line, const char *func);

void print_all_errors(void);

void clear_errors(void);

const char **
get_errors_messages(size_t *number_of_errors);

int *get_errors_codes(size_t *number_of_errors);

size_t get_errors_count(void);
