#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// tag::errors_api[]
typedef struct operation_result op_result_t;
#define result_t __attribute__((warn_unused_result)) op_result_t*

#define failed(CODE, ...)           \
  errors_push(CODE, ##__VA_ARGS__); \
  return failure_code()
#define verify(CALL) (!(CALL) || errors_get_count())
#define ensure(CALL, ...)                            \
  do {                                               \
    if (verify(CALL)) {                              \
      failed(EINVAL, msg(#CALL " "), ##__VA_ARGS__); \
    }                                                \
  } while (0)

#define failure_code() (op_result_t*)(void*) 0
#define success() (op_result_t*)(void*) 1
#define errors_push(CODE, ...)                           \
  do {                                                   \
    errors_push_new(__FILE__, __LINE__, __func__, CODE); \
    (void)(__VA_ARGS__);                                 \
  } while (0)
#define msg(MSG) errors_append_message(MSG)
#define with(EXPR, FORMAT) \
  errors_append_message(", " #EXPR " = " FORMAT, EXPR)
#define errors_assert_empty()                                   \
  do {                                                          \
    if (errors_get_count()) {                                   \
      errors_push(                                              \
          EINVAL,                                               \
          msg("Invalid state when there are unnoticed errors"), \
          with(__func__, "%s"));                                \
      return failure_code();                                    \
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
// end::errors_api[]

// tag::defer[]
// <1>
typedef struct cancel_defer {
  void** target;
  size_t* cancelled;
} cancel_defer_t;

#define CONCAT_(x, y) x##y
#define CONCAT(x, y) CONCAT_(x, y)

// <2>
#define try_defer(func, var, cancel_marker)                  \
  cancel_defer_t CONCAT(defer_, __COUNTER__)                 \
      __attribute__((unused, __cleanup__(defer_##func))) = { \
          .target = (void*)&var, .cancelled = &cancel_marker};

// <3>
#define defer(func, var)                                     \
  cancel_defer_t CONCAT(defer_, __COUNTER__) __attribute__(( \
      unused, __cleanup__(defer_##func))) = {.target = (void*)&var};

// <4>
#define enable_defer(func) enable_defer_imp(func, 0, (void*), "%p")

#define enable_defer_imp(func, failcode, convert, format) \
  static inline void defer_##func(cancel_defer_t* cd) {   \
    if (cd->cancelled && *cd->cancelled) return;          \
    if (func(convert(cd->target)) == failcode) {          \
      errors_push(EINVAL, msg("Failure on " #func),       \
                  with(convert(*cd->target), format));    \
    }                                                     \
  }                                                       \
  void enable_semicolon_after_macro_##__LINE__(void)

// end::defer[]

// tag::defer_free[]
static inline void defer_free(cancel_defer_t* cd) {
  if (cd->cancelled && *cd->cancelled) return;
  free(*cd->target);
}
// end::defer_free[]

// tag::mem_usage[]
result_t mem_alloc(void** buffer, size_t size);
result_t mem_calloc(void** buffer, size_t size);
result_t mem_realloc(void** buffer, size_t new_size);
result_t mem_alloc_page_aligned(void** buffer, size_t size);
result_t mem_duplicate_string(char** dest, const char* src);
// end::mem_usage[]
