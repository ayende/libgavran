#pragma once

#include "defer.h"
#include "errors.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct pal_file_handle file_handle_t;

result_t palfs_compute_handle_size(const char *path, size_t *required_size);

result_t palfs_create_file(const char *path, file_handle_t *handle);

const char *palfs_get_filename(file_handle_t *handle);

result_t palfs_set_file_minsize(file_handle_t *handle, uint64_t minimum_size);

result_t palfs_close_file(file_handle_t *handle);

result_t palfs_get_filesize(file_handle_t *handle, uint64_t *size);

result_t palfs_mmap(file_handle_t *handle, uint64_t offset, uint64_t size,
                    void **address);

result_t palfs_unmap(void *address, uint64_t size);

result_t palfs_write_file(file_handle_t *handle, uint64_t offset,
                          const char *buffer, size_t len_to_write);

// <1>
void defer_palfs_close_file(struct cancel_defer *cd);

// <2>
struct unmap_defer_ctx {
  void *addr;
  uint64_t size;
};

// <3>
void defer_palfs_unmap(struct cancel_defer *cd);
