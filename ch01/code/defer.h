#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

// <1>
struct cancel_defer {
  void* target;
  size_t* cancelled;
};

// <2>
void defer_close(struct cancel_defer* cd);

// <3>
static inline void defer_free(struct cancel_defer* cd) {
  if (cd->cancelled && *cd->cancelled) return;
  free(cd->target);
}

#define CONCAT_(x, y) x##y
#define CONCAT(x, y) CONCAT_(x, y)

// <4>
#define try_defer(func, var, cancel_marker)            \
  struct cancel_defer CONCAT(__DEFER__, __LINE__)      \
      __attribute__((__cleanup__(defer_##func))) = {   \
          .target = var, .cancelled = &cancel_marker}; \
  (void)CONCAT(__DEFER__, __LINE__)

// <5>
#define defer(func, var)                                            \
  struct cancel_defer CONCAT(__DEFER__, __LINE__)                   \
      __attribute__((__cleanup__(defer_##func))) = {.target = var}; \
  (void)CONCAT(__DEFER__, __LINE__)
