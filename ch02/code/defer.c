#include <errno.h>

#include "defer.h"

result_t allocate(void** buffer, size_t size) {
  void* tmp_buf = malloc(size);
  if (!tmp_buf) {
    failed(ENOMEM, msg("Unable to allocate buffer"),
           with(size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}

result_t reallocate(void** buffer, size_t new_size) {
  void* tmp_buf = realloc(*buffer, new_size);
  if (!tmp_buf) {
    failed(ENOMEM, msg("Unable to re-allocate buffer"),
           with(new_size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}

result_t allocate_clear(void** buffer, size_t size) {
  void* tmp_buf = calloc(1, size);
  if (!tmp_buf) {
    failed(ENOMEM, msg("Unable to allocate buffer"),
           with(size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}
