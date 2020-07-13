#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#include "errors.h"

// <1>
struct cancel_defer {
  void* target;
  size_t* cancelled;
};

// <2>
static inline void defer_free(struct cancel_defer* cd) {
  if (cd->cancelled && *cd->cancelled) return;
  free(cd->target);
}

static inline void defer_free_p(struct cancel_defer* cd) {
  // free void** via indirection, not void* directly
  if (cd->cancelled && *cd->cancelled) return;
  free(*(void**)cd->target);
}

#define CONCAT_(x, y) x##y
#define CONCAT(x, y) CONCAT_(x, y)

// <3>
#define try_defer(func, var, cancel_marker)                  \
  struct cancel_defer CONCAT(defer_, __COUNTER__)            \
      __attribute__((unused, __cleanup__(defer_##func))) = { \
          .target = var, .cancelled = &cancel_marker};

// <4>
#define defer(func, var)                                         \
  struct cancel_defer CONCAT(defer_, __COUNTER__) __attribute__( \
      (unused, __cleanup__(defer_##func))) = {.target = var};

// <5>
#define enable_defer(func) enable_defer_imp(func, 0, (void*), "%p")

#define enable_defer_imp(func, failcode, convert, format)    \
  static inline void defer_##func(struct cancel_defer* cd) { \
    if (cd->cancelled && *cd->cancelled) return;             \
    if (func(convert(cd->target)) == failcode) {             \
      errors_push(EINVAL, msg("Failure on " #func),          \
                  with(convert(cd->target), format));        \
    }                                                        \
  }                                                          \
  void enable_semicolon_after_macro_##__LINE__(void)
