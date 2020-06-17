#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"

static result_t handle_newly_opened_database(db_t *db) {
  (void)db;
  // for now, nothing to do here.
  return success();
}

static result_t validate_options(database_options_t *options) {
  if (!options->minimum_size) options->minimum_size = 128 * 1024;

  if (options->minimum_size < 128 * 1024) {
    failed(EINVAL,
           msg("The minimum_size cannot be less than the minimum "
               "value of 128KB"),
           with(options->minimum_size, "%lu"));
  }

  if (options->minimum_size % PAGE_SIZE) {
    failed(EINVAL, msg("The minimum size must be page aligned: "),
           with(PAGE_SIZE, "%d"));
  }

  return success();
}

result_t db_create(const char *path, database_options_t *options,
                   db_t *db) {
  database_options_t default_options = {.minimum_size = 128 * 1024};

  db_state_t *ptr = 0;
  size_t done = 0;

  if (options) {
    ensure(validate_options(options));
  } else {
    options = &default_options;
  }

  size_t db_state_size;
  ensure(palfs_compute_handle_size(path, &db_state_size));
  db_state_size += sizeof(db_state_t);

  ptr = calloc(1, db_state_size);
  ensure(ptr, msg("Unable to allocate database state struct"),
         with(path, "%s"));
  try_defer(free, ptr, done);

  ptr->handle = (file_handle_t *)(ptr + 1);
  memcpy(&ptr->options, options, sizeof(database_options_t));

  ensure(palfs_create_file(path, ptr->handle), with(path, "%s"));
  try_defer(palfs_close_file, ptr->handle, done);

  ensure(palfs_set_file_minsize(ptr->handle, options->minimum_size));

  ensure(palfs_get_filesize(ptr->handle, &ptr->mmap.size));
  ensure(palfs_mmap(ptr->handle, 0, &ptr->mmap));
  try_defer(palfs_unmap, &ptr->mmap, done);

  db->state = ptr;

  ensure(handle_newly_opened_database(db));

  done = 1;  // no need to do resource cleanup
  return success();
}

result_t db_close(db_t *db) {
  if (!db->state) return success();  // double close?

  // even if we failed, need to handle
  // disposal of rest of the system
  bool failure = !palfs_unmap(&db->state->mmap);
  failure |= !palfs_close_file(db->state->handle);

  if (failure) {
    errors_push(EIO, msg("Unable to properly close the database"),
                with(palfs_get_filename(db->state->handle), "%s"));
  }

  free(db->state);
  db->state = 0;

  if (failure) {
    return failure_code();
  }
  return success();
}
