#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include <gavran/infrastructure.h>

// tag::mem_usage[]
result_t mem_alloc(void** buffer, size_t size) {
  void* tmp_buf = malloc(size);
  if (!tmp_buf) {
    failed(ENOMEM, msg("Unable to allocate buffer"),
           with(size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}

result_t mem_alloc_page_aligned(void** buffer, size_t size) {
  void* tmp_buf;
  int err = posix_memalign(&tmp_buf, 4096, size);
  if (err) {
    failed(err, msg("Unable to allocate page aligned buffer"),
           with(size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}

result_t mem_realloc(void** buffer, size_t new_size) {
  void* tmp_buf = realloc(*buffer, new_size);
  if (!tmp_buf) {
    failed(ENOMEM, msg("Unable to re-allocate buffer"),
           with(new_size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}
// end::mem_usage[]

result_t mem_calloc(void** buffer, size_t size) {
  void* tmp_buf = calloc(1, size);
  if (!tmp_buf) {
    failed(ENOMEM, msg("Unable to allocate buffer"),
           with(size, "%zu"));
  }
  *buffer = tmp_buf;
  return success();
}

result_t mem_duplicate_string(char** dest, const char* src) {
  *dest = strdup(src);
  if (!*dest) {
    failed(ENOMEM, msg("Unable to duplicate string"),
           with(src, "%s"));
  }
  return success();
}
