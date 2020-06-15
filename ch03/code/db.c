#include <errno.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "pal.h"
#include "platform.fs.h"

struct database_state {
  database_options_t options;
  struct mmap_args mmap;
  file_handle_t *handle;
};

static result_t handle_newly_opened_database(db_t *db) {
  // for now, nothing to do here.
  success();
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

  return true;
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
  ensure(get_file_handle_size(path, &db_state_size));
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
  return true;
}