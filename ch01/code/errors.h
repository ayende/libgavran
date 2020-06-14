#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct operation_result op_result_t;                          

#define result_t __attribute__((warn_unused_result)) op_result_t*     

#define failed(CODE, MSG, ...)                                       \
  push_error(CODE, MSG);                                             \
  __VA_ARGS__;                                                       \
  return (op_result_t*)(void*)0;

#define failure(CODE, MSG, ...)                                      \
  (push_error_internal(__FILE__, __LINE__, __func__, CODE, MSG),     \
   ##__VA_ARGS__,                                                    \
   (op_result_t*)(void*)0)

#define ensure(CALL, ...)                                            \
  if (!CALL) {                                                       \
    return failure(EINVAL, #CALL, ##__VA_ARGS__);                    \
  }

#define msg(MSG) errors_append_message(MSG)                           

#define with(EXPR, FORMAT)                                           \
  errors_append_message(", " #EXPR " = " FORMAT, EXPR)

#define success() return (op_result_t*)(void*)1                       

#define errors_push(CODE, MSG)                                       \
  errors_push_new(                                                   \
    __FILE__, __LINE__, __func__, CODE, MSG);

#define errors_assert_empty()                                        \
  if (get_errors_count()) {                                          \
    push_error(EINVAL,                                               \
               "Cannot call %s when there are unnoticed errors",     \
               __func__);                                            \
  }

__attribute__((__format__(__printf__, 5, 6))) op_result_t*            
errors_push_new(const char* file,
                uint32_t line,
                const char* func,
                int32_t code,
                const char* user_message);

__attribute__((__format__(__printf__, 1, 2))) op_result_t*            
errors_append_message(const char* format, ...);

void
errors_print_all(void);                                               

void
errors_clear(void);                                                   

const char**
errors_get_messages(size_t* number_of_errors);                        

int*
errors_get_codes(size_t* number_of_errors);                           

size_t
errors_get_count(void);

uint32_t 
errors_get_oom_flag(void);