#pragma once

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

struct cancel_defer {                                                   // <1>
  void *target;
  size_t *cancelled;
};

void defer_close(struct cancel_defer *cd);                             // <2>

static inline void defer_free(struct cancel_defer *cd) {               // <3>
  if (cd->cancelled && *cd->cancelled)
    return;
  void *p = *(void **)cd->target;
  free(p);
}

#define try_defer(func, var, cancel_marker)                           \// <4>
  struct cancel_defer _##__LINE__                                     \
      __attribute__((__cleanup__(defer_##func))) = {                  \
          .target = var, .cancelled = &cancel_marker};                \
  (void)_##__LINE__;

#define defer(func, var)                                              \ // <5>
  struct cancel_defer __DEFER__##__LINE__                             \
      __attribute__((__cleanup__(defer_##func))) = {.target = var};   \
  (void)__DEFER__##__LINE__;
