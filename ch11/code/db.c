#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

static inline result_t
get_number_of_free_space_bitmap_pages(uint64_t number_of_pages,
                                      uint32_t *size) {
  uint64_t r =
      number_of_pages / BITS_IN_PAGE + (number_of_pages % BITS_IN_PAGE ? 1 : 0);
  // <2>
  ensure(r <= UINT32_MAX,
         msg("650KB should be enough for everyone, but you wanted more than "
             "2 exabytes?"),
         with(number_of_pages, "%lu"));
  *size = (uint32_t)r;
  return success();
}

// tag::db_find_next_db_size[]
uint64_t db_find_next_db_size(uint64_t current, uint64_t requested_size) {
  uint64_t uint_of_growth = next_power_of_two(current / 10);
  uint64_t suggested = uint_of_growth;
  if (suggested > 1024 * 1024 * 1024)
    suggested = 1024 * 1024 * 1024;
  while (suggested <= requested_size) {
    suggested += uint_of_growth;
  }
  if (suggested < (1024 * 1024))
    suggested = 1024 * 1024;

  uint64_t next_p2_file = next_power_of_two(current + suggested);
  if (next_p2_file < current + uint_of_growth * 2)
    return next_p2_file;

  return current + suggested;
}
// end::db_find_next_db_size[]

// tag::db_move_free_space_bitmap[]
static result_t db_move_free_space_bitmap(txn_t *tx, uint64_t from, uint64_t to,
                                          page_t *free_space) {
  // need to move to new location, cannot allocate, because we might be already
  // allocating not sure yet _where_ to
  // <1>
  uint32_t required_pages;
  ensure(get_number_of_free_space_bitmap_pages(to, &required_pages));

  required_pages += next_power_of_two(required_pages / 10);

  // <2>
  void *new_freespace_map;
  ensure(palmem_allocate_pages(&new_freespace_map, required_pages));
  defer(free, new_freespace_map);
  // <3>
  memset(new_freespace_map, INT32_MAX, required_pages * PAGE_SIZE); // set busy
  memcpy(new_freespace_map, free_space->address, free_space->overflow_size);
  // <4>
  for (uint64_t i = from; i < to; i++) {
    clear_bit(new_freespace_map, i); // mark the new available pages as free
  }

  // <5>
  bitmap_search_state_t search;
  init_search(&search, new_freespace_map,
              required_pages * PAGE_SIZE / sizeof(uint64_t), required_pages);
  if (!search_free_range_in_bitmap(&search)) {
    failed(ENOSPC,
           msg("Could not find enough room for free space even after we "
               "allocated new size"),
           with(required_pages, "%d"));
  }
  uint64_t end_of_free_space =
      search.found_position + free_space->overflow_size / PAGE_SIZE;
  // <6>
  // mark the new free space pages as busy
  for (uint64_t i = search.found_position; i <= end_of_free_space; i++) {
    set_bit(new_freespace_map, i);
  }

  page_t new_free_space_page = {.page_num = search.found_position,
                                .overflow_size = required_pages * PAGE_SIZE};
  // <7>
  ensure(txn_modify_page(tx, &new_free_space_page));
  memcpy(new_free_space_page.address, new_freespace_map,
         new_free_space_page.overflow_size);
  // <8>
  page_metadata_t *free_space_metadata;
  ensure(txn_modify_metadata(tx, search.found_position, &free_space_metadata));
  free_space_metadata->type = page_free_space_bitmap;
  free_space_metadata->overflow_size = required_pages * PAGE_SIZE;
  // <9>
  tx->state->global_state.header.free_space_bitmap_start =
      search.found_position;

  // <10>
  ensure(txn_free_page(tx, free_space)); // release the old space

  return success();
}
// end::db_move_free_space_bitmap[]

// tag::db_finalize_file_size_increase[]
static result_t db_increase_free_space_bitmap(txn_t *tx, uint64_t from,
                                              uint64_t to) {
  page_t free_space = {
      .page_num = tx->state->global_state.header.free_space_bitmap_start};
  ensure(txn_modify_page(tx, &free_space));

  if (size_to_pages(free_space.overflow_size) * BITS_IN_PAGE > to) {
    // can do an in place update
    for (uint64_t i = from; i < to; i++) {
      clear_bit(free_space.address, i);
    }
    return success();
  }
  // need to move to a new location
  return db_move_free_space_bitmap(tx, from, to, &free_space);
}

static result_t db_finalize_file_size_increase(txn_t *tx, uint64_t from,
                                               uint64_t to) {
  ensure(db_increase_free_space_bitmap(tx, from, to));
  page_metadata_t *file_header_metadata;
  ensure(txn_modify_metadata(tx, 0, &file_header_metadata));
  tx->state->global_state.header.number_of_pages = to;
  memcpy(&file_header_metadata->file_header, &tx->state->global_state.header,
         sizeof(file_header_t));

  return success();
}
// end::db_finalize_file_size_increase[]

// tag::db_try_increase_file_size[]
static void db_clear_old_mmap(void *state) {
  struct mmap_args *args = state;
  // we don't have a good way to respond to an error
  // here, will be reported via the push_error, instead
  (void)palfs_unmap(args);
}

static result_t db_new_size_can_fit_free_space_bitmap(uint64_t current_size,
                                                      uint64_t *new_size) {
  uint32_t required_pages;
  ensure(get_number_of_free_space_bitmap_pages(*new_size / PAGE_SIZE,
                                               &required_pages));
  required_pages *= 2;
  if (*new_size - current_size > required_pages * PAGE_SIZE)
    return success();
  *new_size += required_pages * PAGE_SIZE;
  return success();
}

result_t db_try_increase_file_size(txn_t *tx, uint64_t pages) {
  uint64_t new_size = db_find_next_db_size(tx->state->global_state.mmap.size,
                                           pages * PAGE_SIZE);

  ensure(db_new_size_can_fit_free_space_bitmap(
      tx->state->global_state.mmap.size, &new_size));
  ensure(new_size < tx->state->db->options.maximum_size,
         msg("Unable to grow the database beyond the maximum size"),
         with(new_size, "%lu"), with(pages, "%lu"),
         with(tx->state->db->options.maximum_size, "%lu"));

  file_handle_t *handle = tx->state->db->handle;
  ensure(palfs_set_file_minsize(handle, new_size));

  uint64_t from = tx->state->global_state.header.number_of_pages;
  uint64_t to = new_size / PAGE_SIZE;
  struct mmap_args new_map = {.size = new_size};
  ensure(palfs_mmap(handle, 0, &new_map), msg("Unable to map the file again"),
         with(new_map.size, "%lu"));
  {
    size_t cancel_defer = 0;
    try_defer(palfs_unmap, &new_map, cancel_defer);

    // discard new map if we failed to commit
    ensure(txn_register_on_rollback(tx->state, db_clear_old_mmap, &new_map,
                                    sizeof(struct mmap_args)));
    cancel_defer = 1;
  }
  // discard old map when no one is looking at this tx
  ensure(txn_register_on_forget(tx->state, db_clear_old_mmap,
                                &tx->state->global_state.mmap,
                                sizeof(struct mmap_args)));

  tx->state->global_state.mmap = new_map;
  return db_finalize_file_size_increase(tx, from, to);
}

// end::db_try_increase_file_size[]

static result_t initialize_freespace_bitmap(txn_t *tx, page_t *metadata_page) {
  tx->state->global_state.header.free_space_bitmap_start = 1;

  uint32_t free_space_size_in_pages;
  ensure(get_number_of_free_space_bitmap_pages(
      tx->state->global_state.header.number_of_pages,
      &free_space_size_in_pages));

  uint32_t num_of_busy_pages =
      free_space_size_in_pages + 1; // for the header page

  uint64_t start = tx->state->global_state.header.free_space_bitmap_start;

  // <3>
  page_metadata_t *entries = metadata_page->address;
  entries[start].type = page_free_space_bitmap;
  entries[start].overflow_size = PAGE_SIZE * free_space_size_in_pages;

  while (num_of_busy_pages > BITS_IN_PAGE) {
    // if we allocate a large file upfront, we may need to
    // allocate a lot of pages for the free space bitmap
    // than can be fit on a single page, that is relevant
    // for files > 4TB in size, but let's handle this scenario
    num_of_busy_pages -= BITS_IN_PAGE;
    page_t full = {.page_num = start++};
    ensure(txn_modify_page(tx, &full));
    // set the whole page as set bits
    memset(full.address, -1, PAGE_SIZE);
  }

  page_t p = {.page_num = start};
  ensure(txn_modify_page(tx, &p));
  for (size_t i = 0; i < num_of_busy_pages; i++) {
    set_bit(p.address, i);
  }
  // mark as busy the pages beyond the end of the file
  for (size_t i = tx->state->global_state.header.number_of_pages % BITS_IN_PAGE;
       i < BITS_IN_PAGE; i++) {
    set_bit(p.address, i);
  }

  return success();
}

static result_t initialize_file_structure(db_t *db) {
  txn_t tx;
  ensure(txn_create(db, TX_WRITE, &tx));
  defer(txn_close, &tx);

  page_t metadata_page = {.page_num = 0};
  ensure(txn_modify_page(&tx, &metadata_page));
  memset(metadata_page.address, 0, PAGE_SIZE);
  // <4>
  page_metadata_t *entry = metadata_page.address;
  entry->type = page_metadata;
  entry->overflow_size = PAGE_SIZE;

  ensure(initialize_freespace_bitmap(&tx, &metadata_page));

  // <5>
  memcpy(&entry->file_header, &tx.state->global_state.header,
         sizeof(file_header_t));

  ensure(txn_commit(&tx));
  return success();
}
// end::initialize_file_structure[]

// tag::handle_newly_opened_database[]
static result_t handle_newly_opened_database(db_t *db) {
  ensure(wal_open_and_recover(db));

  size_t cancel_defer = 0;
  try_defer(wal_close, db->state, cancel_defer);
  db_state_t *state = db->state;
  // at this point state->header is zeroed
  // if the file header is zeroed, we are probably dealing with a new
  // database

  bool isNew = true;

  {
    txn_t tx;
    ensure(txn_create(db, TX_READ, &tx));
    defer(txn_close, &tx);
    page_t first_page = {.page_num = 0};
    ensure(txn_get_page(&tx, &first_page));
    page_metadata_t *entry = first_page.address;
    file_header_t *file_header = &entry->file_header;
    file_header_t zeroed;
    memset(&zeroed, 0, sizeof(file_header_t));
    isNew = !memcmp(file_header, &zeroed, sizeof(file_header_t));
  }

  if (isNew) {
    // now needs to set it up
    state->global_state.header.last_tx_id = 0;
    state->global_state.header.version = 1;
    state->global_state.header.page_size_power_of_two =
        (uint8_t)(log2(PAGE_SIZE));
    state->global_state.header.number_of_pages =
        state->global_state.mmap.size / PAGE_SIZE;

    ensure(initialize_file_structure(db));
  }

  {
    // need a new tx here, because we might have committed changes to init the
    // file
    txn_t tx;
    ensure(txn_create(db, TX_READ, &tx));
    defer(txn_close, &tx);
    page_t first_page = {.page_num = 0};
    ensure(txn_get_page(&tx, &first_page));
    page_metadata_t *entry = first_page.address;
    file_header_t *file_header = &entry->file_header;

    ensure(entry->type == page_metadata,
           msg("The first file page must be metadata page, but was..."),
           with(entry->type, "%x"));

    ensure(file_header->number_of_pages * PAGE_SIZE <=
               state->global_state.mmap.size,
           EINVAL,
           msg("The size of the file is smaller than the expected size, "
               "file was probably truncated"),
           with(palfs_get_filename(state->handle), "%s"),
           with(state->global_state.mmap.size, "%lu"),
           with(file_header->number_of_pages * PAGE_SIZE, "%lu"));

    ensure(pow(2, file_header->page_size_power_of_two) == PAGE_SIZE, EINVAL,
           msg("The file page size is invalid"),
           with(palfs_get_filename(state->handle), "%s"),
           with((uint32_t)(pow(2, file_header->page_size_power_of_two)), "%d"),
           with(PAGE_SIZE, "%d"));

    // the last_tx_id is stored only in the WAL, so we need to keep it's value
    uint64_t last_tx_id = db->state->global_state.header.last_tx_id;
    memcpy(&db->state->global_state.header, file_header, sizeof(file_header_t));
    db->state->global_state.header.last_tx_id = last_tx_id;
  }

  cancel_defer = 1;

  return success();
}
// end::handle_newly_opened_database[]

static result_t validate_options(database_options_t *user_options,
                                 database_options_t *owned_options) {
  if (user_options->minimum_size)
    owned_options->minimum_size = user_options->minimum_size;
  if (user_options->wal_size)
    owned_options->wal_size = user_options->wal_size;
  if (user_options->maximum_size)
    owned_options->maximum_size = user_options->maximum_size;
  memcpy(owned_options->encryption_key, user_options->encryption_key,
         crypto_aead_aes256gcm_KEYBYTES);
  owned_options->encrypted = !sodium_is_zero(owned_options->encryption_key,
                                             crypto_aead_aes256gcm_KEYBYTES);

  if (owned_options->wal_size % PAGE_SIZE) {
    failed(EINVAL, msg("The wal size must be page aligned: "),
           with(owned_options->wal_size, "%lu"));
  }

  if (owned_options->minimum_size < 128 * 1024) {
    failed(EINVAL,
           msg("The minimum_size cannot be less than the minimum "
               "value of 128KB"),
           with(owned_options->minimum_size, "%lu"));
  }

  if (owned_options->minimum_size > owned_options->maximum_size) {
    failed(EINVAL,
           msg("The minimum_size cannot be greater than the maximum size"),
           with(owned_options->minimum_size, "%lu"),
           with(owned_options->maximum_size, "%lu"));
  }

  if (owned_options->minimum_size % PAGE_SIZE) {
    failed(EINVAL, msg("The minimum size must be page aligned: "),
           with(owned_options->minimum_size, "%lu"), with(PAGE_SIZE, "%d"));
  }

  return success();
}

// tag::db_setup_page_validation[]
static result_t db_setup_page_validation(db_state_t *ptr) {
  if (ptr->options.page_validation == page_validation_once) {
    ptr->original_number_of_pages = ptr->global_state.header.number_of_pages;
    ptr->first_read_bitmap =
        calloc(ptr->global_state.header.number_of_pages * PAGE_SIZE / 64,
               sizeof(uint64_t));
    ensure(ptr->first_read_bitmap,
           msg("Unable to allocate database's first read bitamp"));
  }
  return success();
}
// end::db_setup_page_validation[]

result_t db_create(const char *path, database_options_t *options, db_t *db) {
  database_options_t owned_options = {.minimum_size = 128 * 1024,
                                      .wal_size = 16 * 1024,
                                      .maximum_size = UINT64_MAX,
                                      .page_validation = page_validation_once};

  db_state_t *ptr = 0;
  size_t done = 0;
  if (options) {
    ensure(validate_options(options, &owned_options));
  }

  // tag::default_read_tx[]
  size_t db_state_size;
  ensure(palfs_compute_handle_size(path, &db_state_size));
  db_state_size += sizeof(db_state_t);
  // <2>
  db_state_size += sizeof(txn_state_t);

  ptr = calloc(1, db_state_size);
  ensure(ptr, msg("Unable to allocate database state struct"),
         with(path, "%s"));
  db->state = ptr;
  try_defer(db_close, db, done);

  ptr->handle =
      (file_handle_t *)((char *)ptr + sizeof(txn_state_t) + sizeof(db_state_t));
  memcpy(&ptr->options, &owned_options, sizeof(database_options_t));

  // <3>
  ptr->default_read_tx = (txn_state_t *)(ptr + 1);

  ptr->default_read_tx->pages = 0;
  ptr->default_read_tx->db = ptr;
  // only the default read tx has this setup
  ptr->default_read_tx->flags = TX_READ | TX_COMMITED;
  ptr->default_read_tx->can_free_after_tx_id = UINT64_MAX;
  ptr->last_write_tx = ptr->default_read_tx;
  // end::default_read_tx[]

  ensure(palfs_create_file(path, ptr->handle, palfs_file_creation_flags_none),
         with(path, "%s"));
  try_defer(palfs_close_file, ptr->handle, done);

  ensure(palfs_set_file_minsize(ptr->handle, options->minimum_size));

  ensure(palfs_get_filesize(ptr->handle, &ptr->global_state.mmap.size));
  ensure(palfs_mmap(ptr->handle, 0, &ptr->global_state.mmap));
  try_defer(palfs_unmap, &ptr->global_state.mmap, done);

  ensure(handle_newly_opened_database(db));

  ensure(db_setup_page_validation(ptr));

  done = 1; // no need to do resource cleanup
  return success();
}

result_t db_close(db_t *db) {
  if (!db->state)
    return success(); // double close?

  // even if we failed, need to handle
  // disposal of rest of the system
  bool failure = !wal_close(db->state);
  failure |= !palfs_unmap(&db->state->global_state.mmap);
  failure |= !palfs_close_file(db->state->handle);

  if (failure) {
    errors_push(EIO, msg("Unable to properly close the database"));
  }

  while (db->state->transactions_to_free) {
    txn_state_t *cur = db->state->transactions_to_free;
    db->state->transactions_to_free = cur->next_tx;
    if (db->state->default_read_tx == cur)
      continue; // can't free the default one
    txn_free_single_tx(cur);
  }
  free(db->state->first_read_bitmap);
  free(db->state);
  db->state = 0;

  if (failure) {
    return failure_code();
  }
  return success();
}

result_t TEST_db_get_map_at(db_t *db, uint64_t page_num, void **address) {
  *address =
      (char *)db->state->global_state.mmap.address + (page_num * PAGE_SIZE);
  return success();
}
