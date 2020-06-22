#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "errors.h"
#include "impl.h"
#include "platform.fs.h"
#include "platform.mem.h"

// tag::initialize_file_structure[]
static inline result_t get_number_of_free_space_bitmap_pages(db_state_t *db,
                                                             uint32_t *size) {
  uint64_t r = db->header.number_of_pages / BITS_IN_PAGE +
               (db->header.number_of_pages % BITS_IN_PAGE ? 1 : 0);
  // <2>
  ensure(r <= UINT32_MAX,
         msg("650KB should be enough for everyone, but you wanted more than "
             "2 exabytes?"),
         with(db->header.number_of_pages, "%lu"));
  *size = (uint32_t)r;
  return success();
}

static result_t initialize_freespace_bitmap(db_t *db, txn_t *tx,
                                            page_t *metadata_page) {
  db->state->header.free_space_bitmap_start = 1;

  uint32_t free_space_size_in_pages;
  ensure(get_number_of_free_space_bitmap_pages(db->state,
                                               &free_space_size_in_pages));

  uint32_t num_of_busy_pages =
      free_space_size_in_pages + 1; // for the header page

  uint64_t start = db->state->header.free_space_bitmap_start;

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
  for (size_t i = db->state->header.number_of_pages % BITS_IN_PAGE;
       i < BITS_IN_PAGE; i++) {
    set_bit(p.address, i);
  }

  return success();
}

static result_t initialize_file_structure(db_t *db) {
  txn_t tx;
  ensure(txn_create(db, 0, &tx));
  defer(txn_close, &tx);

  page_t metadata_page = {.page_num = 0};
  ensure(txn_modify_page(&tx, &metadata_page));
  memset(metadata_page.address, 0, PAGE_SIZE);
  // <4>
  page_metadata_t *entry = metadata_page.address;
  entry->type = page_metadata;

  ensure(initialize_freespace_bitmap(db, &tx, &metadata_page));

  // <5>
  memcpy(&entry->file_header, &db->state->header, sizeof(file_header_t));

  ensure(txn_commit(&tx));
  return success();
}
// end::initialize_file_structure[]

// tag::handle_newly_opened_database[]
static result_t handle_newly_opened_database(db_t *db) {
  db_state_t *state = db->state;
  page_metadata_t *entry = state->mmap.address;
  file_header_t *file_header = &entry->file_header;
  // at this point state->header is zeroed
  // if the file headeris zeroed, we are probably dealing with a new
  // database
  bool isNew = !memcmp(file_header, &state->header, sizeof(file_header_t)) &&
               !entry->type;

  if (isNew) {
    // now needs to set it up
    state->header.magic = FILE_HEADER_MAGIC;
    state->header.version = 1;
    state->header.page_size_power_of_two = (uint8_t)(log2(PAGE_SIZE));
    state->header.number_of_pages = state->mmap.size / PAGE_SIZE;
    ensure(initialize_file_structure(db));
  }

  ensure(entry->type == page_metadata,
         msg("The first file page must be metadata page, but was..."),
         with(entry->type, "%x"));

  ensure(file_header->magic == FILE_HEADER_MAGIC, EINVAL,
         msg("Unable to find valid file header"),
         with(palfs_get_filename(state->handle), "%s"),
         with(file_header->magic, "%lu"));

  ensure(file_header->number_of_pages * PAGE_SIZE <= state->mmap.size, EINVAL,
         msg("The size of the file is smaller than the expected size, "
             "file was probably truncated"),
         with(palfs_get_filename(state->handle), "%s"),
         with(state->mmap.size, "%lu"),
         with(file_header->number_of_pages * PAGE_SIZE, "%lu"));

  ensure(pow(2, file_header->page_size_power_of_two) == PAGE_SIZE, EINVAL,
         msg("The file page size is invalid"),
         with(palfs_get_filename(state->handle), "%s"),
         with((uint32_t)(pow(2, file_header->page_size_power_of_two)), "%d"),
         with(PAGE_SIZE, "%d"));

  memcpy(&db->state->header, file_header, sizeof(file_header_t));
  return success();
}
// end::handle_newly_opened_database[]

static result_t validate_options(database_options_t *options) {
  if (!options->minimum_size)
    options->minimum_size = 128 * 1024;

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

result_t db_create(const char *path, database_options_t *options, db_t *db) {
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

  done = 1; // no need to do resource cleanup
  return success();
}

result_t db_close(db_t *db) {
  if (!db->state)
    return success(); // double close?

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
