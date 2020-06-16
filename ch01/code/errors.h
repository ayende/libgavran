#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct operation_result op_result_t;

#define result_t __attribute__((warn_unused_result)) op_result_t*

#define failed(CODE, ...)           \
  errors_push(CODE, ##__VA_ARGS__); \
  return (op_result_t*)(void*)0

#define ensure(CALL, ...)                        \
  do {                                           \
    if (!CALL || errors_get_count()) {           \
      failed(EINVAL, msg(#CALL), ##__VA_ARGS__); \
    }                                            \
  } while (0)

#define msg(MSG) errors_append_message(MSG)

#define with(EXPR, FORMAT) \
  errors_append_message(", " #EXPR " = " FORMAT, EXPR)

#define success() (op_result_t*)(void*) 1

#define errors_push(CODE, ...)                           \
  do {                                                   \
    errors_push_new(__FILE__, __LINE__, __func__, CODE); \
    (void)(__VA_ARGS__);                                 \
  } while (0)

#define errors_assert_empty()                                   \
  do {                                                          \
    if (errors_get_count()) {                                   \
      errors_push(                                              \
          EINVAL,                                               \
          msg("Invalid state when there are unnoticed errors"), \
          with(__func__, "%s"));                                \
    }                                                           \
  } while (0)

op_result_t* errors_push_new(const char* file, uint32_t line,
                             const char* func, int32_t code);

__attribute__((__format__(__printf__, 1, 2))) op_result_t*
errors_append_message(const char* format, ...);

void errors_print_all(void);

void errors_clear(void);

const char** errors_get_messages(size_t* number_of_errors);

int* errors_get_codes(size_t* number_of_errors);

size_t errors_get_count(void);

uint32_t errors_get_oom_flag(void);
