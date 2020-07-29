#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::db_create[]
result_t db_create(const char *path, db_options_t *options,
                   db_t *db) {
  db_options_t owned_options;
  db_initialize_default_options(&owned_options);
  if (options) {
    ensure(db_validate_options(options, &owned_options));
  }
  size_t done = 0;
  ensure(mem_calloc((void *)&db->state, sizeof(db_state_t)));
  try_defer(db_close, *db, done);
  ensure(pal_create_file(path, &db->state->handle,
                         pal_file_creation_flags_none));
  memcpy(&db->state->options, &owned_options, sizeof(db_options_t));
  ensure(pal_set_file_size(db->state->handle,
                           owned_options.minimum_size, UINT64_MAX));
  db->state->map.size = db->state->handle->size;
  if (!(owned_options.flags & db_flags_avoid_mmap_io)) {
    ensure(pal_mmap(db->state->handle, 0, &db->state->map));
  }
  ensure(db_initialize_default_read_tx(db->state));
  ensure(wal_open_and_recover(db));
  ensure(db_init(db));
  ensure(db_setup_page_validation(db));
  done = 1;  // no need to do resource cleanup
  return success();
}
// end::db_create[]

// tag::db_validate_options[]
implementation_detail result_t db_validate_options(
    db_options_t *user_options, db_options_t *default_options) {
  if (user_options->minimum_size)
    default_options->minimum_size = user_options->minimum_size;
  if (user_options->maximum_size)
    default_options->maximum_size = user_options->maximum_size;
  if (user_options->wal_size)
    default_options->wal_size = user_options->wal_size;

  if (default_options->minimum_size < 128 * 1024) {
    failed(EINVAL,
           msg("The minimum_size cannot be less than the minimum "
               "value of 128KB"),
           with(default_options->minimum_size, "%lu"));
  }
  if (default_options->minimum_size > default_options->maximum_size) {
    failed(
        EINVAL,
        msg("The maximum_size cannot be less than the minimum_size"),
        with(default_options->maximum_size, "%lu"),
        with(default_options->minimum_size, "%lu"));
  }

  if (default_options->wal_size < 128 * 1024) {
    failed(EINVAL,
           msg("The wal_size cannot be less than the minimum "
               "value of 128KB"),
           with(default_options->wal_size, "%lu"));
  }

  return success();
}
// end::db_validate_options[]

// tag::db_initialize_default_options[]
implementation_detail void db_initialize_default_options(
    db_options_t *options) {
  memset(options, 0, sizeof(db_options_t));
  options->minimum_size = 1024 * 1024;
  options->maximum_size = UINT64_MAX;
  options->wal_size = 256 * 1024;
}
// end::db_initialize_default_options[]

// tag::db_close[]
result_t db_close(db_t *db) {
  if (!db || !db->state) return success();  // double close?

  bool failure = false;
  failure |= !pal_unmap(&db->state->map);
  failure |= !pal_close_file(db->state->handle);
  failure |= !wal_close(db->state);

  if (failure) {
    errors_push(EIO, msg("Unable to properly close the database"));
  }

  while (db->state->last_write_tx &&
         db->state->default_read_tx != db->state->last_write_tx) {
    txn_state_t *cur = db->state->last_write_tx;
    db->state->last_write_tx = cur->prev_tx;
    txn_free_single_tx_state(cur);
  }
  free(db->state->default_read_tx);
  free(db->state);
  db->state = 0;

  if (failure) {
    return failure_code();
  }
  return success();
}
// end::db_close[]
