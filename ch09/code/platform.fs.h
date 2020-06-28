#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "db.h"
#include "defer.h"
#include "errors.h"

typedef struct pal_file_handle file_handle_t;

result_t palfs_compute_handle_size(const char *path, size_t *required_size);
// tag::palfs_create_file[]
enum palfs_file_creation_flags {
  palfs_file_creation_flags_none = 0,
  palfs_file_creation_flags_durable = 1
};

result_t palfs_create_file(const char *path, file_handle_t *handle,
                           enum palfs_file_creation_flags flags);
// end::palfs_create_file[]

const char *palfs_get_filename(file_handle_t *handle);

result_t palfs_set_file_minsize(file_handle_t *handle, uint64_t minimum_size);

result_t palfs_fsync_file(file_handle_t *handle);

result_t palfs_close_file(file_handle_t *handle);

result_t palfs_get_filesize(file_handle_t *handle, uint64_t *size);

// <1>
struct mmap_args {
  void *address;
  size_t size;
};

result_t palfs_mmap(file_handle_t *handle, uint64_t offset,
                    struct mmap_args *m);

result_t palfs_unmap(struct mmap_args *m);

result_t palfs_write_file(file_handle_t *handle, uint64_t offset,
                          const char *buffer, size_t len_to_write);

// <2>
void defer_palfs_close_file(struct cancel_defer *cd);

void defer_palfs_unmap(struct cancel_defer *cd);

result_t TEST_db_get_map_at(db_t *db, uint64_t page_num, void **address);
