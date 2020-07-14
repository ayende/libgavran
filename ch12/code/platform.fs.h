#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "db.h"
#include "defer.h"
#include "errors.h"

typedef struct span {
  void *address;
  size_t size;
} span_t;

typedef struct file_handle {
  void *handle;
  char *filename;
} file_handle_t;

enum palfs_file_creation_flags {
  palfs_file_creation_flags_none = 0,
  palfs_file_creation_flags_durable = 1
};

result_t pal_create_file(const char *path, file_handle_t **handle,
                         enum palfs_file_creation_flags flags);

result_t pal_set_file_minsize(file_handle_t *handle, uint64_t minimum_size);
result_t pal_truncate_file(file_handle_t *handle, uint64_t new_size);
result_t pal_fsync_file(file_handle_t *handle);
result_t pal_close_file(file_handle_t *handle);
result_t pal_get_filesize(file_handle_t *handle, uint64_t *size);

result_t pal_mmap(file_handle_t *handle, uint64_t offset, span_t *span);
result_t pal_enable_writes(span_t *range);
result_t pal_disable_writes(span_t *range);
result_t pal_unmap(span_t *range);

result_t pal_write_file(file_handle_t *handle, uint64_t offset,
                        const char *buffer, size_t len_to_write);
result_t pal_read_file(file_handle_t *handle, uint64_t offset, void *buffer,
                       size_t len_to_read);

result_t pal_allocate_pages(void **p, uint64_t pages);
result_t pal_free_page(void **p);

void defer_pal_close_file(struct cancel_defer *cd);
void defer_pal_unmap(struct cancel_defer *cd);

enable_defer(pal_free_page);
