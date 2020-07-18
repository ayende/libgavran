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
  try_defer(db_close, db, done);
  ensure(pal_create_file(path, &db->state->handle,
                         pal_file_creation_flags_none));
  try_defer(pal_close_file, db->state->handle, done);
  memcpy(&db->state->options, &owned_options, sizeof(db_options_t));
  ensure(pal_set_file_size(db->state->handle,
                           owned_options.minimum_size, UINT64_MAX));
  db->state->global_state.span.size = db->state->handle->size;
  if (!owned_options.avoid_mmap_io) {
    ensure(pal_mmap(db->state->handle, 0,
                    &db->state->global_state.span));
  }
  try_defer(pal_unmap, db->state->global_state.span, done);
  ensure(db_initialize_default_read_tx(db->state));
  ensure(db_init(db));
  ensure(db_setup_page_validation(db->state));
  done = 1;  // no need to do resource cleanup
  return success();
}
// end::db_create[]
