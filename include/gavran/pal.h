#pragma once

#include <stdbool.h>
#include <stdint.h>

#include <gavran/infrastructure.h>

// tag::pal_file_creation_flags[]
enum pal_file_creation_flags {
  pal_file_creation_flags_none = 0,
  pal_file_creation_flags_durable = 1
};
// end::pal_file_creation_flags[]

// tag::pal_api[]
typedef struct span {
  void *address;
  size_t size;
} span_t;

typedef struct pal_file_handle {
  union {
    int fd;
    void *handle;
  };
  char *filename;
  uint64_t size;
} file_handle_t;

// working with files
result_t pal_create_file(const char *path, file_handle_t **handle,
                         enum pal_file_creation_flags flags);
result_t pal_set_file_size(file_handle_t *handle,
                           uint64_t minimum_size,
                           uint64_t maximum_size);
result_t pal_fsync(file_handle_t *handle);
result_t pal_close_file(file_handle_t *handle);
void defer_pal_close_file(struct cancel_defer *cd);

// memory map
result_t pal_mmap(file_handle_t *handle, uint64_t offset,
                  span_t *span);
result_t pal_enable_writes(span_t *range);
void defer_pal_disable_writes(cancel_defer_t *cd);
result_t pal_unmap(span_t *range);
void defer_pal_unmap(cancel_defer_t *cd);

// reading and writing to a file
result_t pal_write_file(file_handle_t *handle, uint64_t offset,
                        const char *buffer, size_t size);
result_t pal_read_file(file_handle_t *handle, uint64_t offset,
                       void *buffer, size_t size);
// end::pal_api[]
