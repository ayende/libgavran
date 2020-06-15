#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

#include "errors.h"
#include "pal.h"
#include "transactions.h"

#define BITS_IN_PAGE (PAGE_SIZE * 8)
#define PAGES_IN_METADATA_PAGE (1024 * 1024)

typedef struct page_hash_entry {
  uint64_t page_num;
  void *address;
} page_hash_entry_t;

struct transaction_state {
  db_state_t *db;
  size_t allocated_size;
  uint32_t flags;
  uint32_t modified_pages;
  page_hash_entry_t entries[];
};

struct database_state {
  database_options_t options;
  file_header_t header;
  void *address;
  uint64_t address_size;
  file_handle_t *handle;
};

enum page_flags {
  page_free = 0,
  page_single = 1,
  page_overflow_first = 2,
  page_overflow_rest = 4,
  page_metadata = 8
} __attribute__((__packed__));
_Static_assert(sizeof(enum page_flags) == 1,
               "Expecting page_flags to be a single char in size");

typedef struct page_metadata {
  union {
    struct {
      uint32_t overflow_size;
      char flags;
      char _padding2[3];
    };
    char _padding[16];
  };
} page_metadata_t;

#define get_number_of_buckets(state)               \
  ((state->allocated_size - sizeof(txn_state_t)) / \
   sizeof(page_hash_entry_t))

static void get_metadata_section_stats(
    uint64_t total_number_of_pages, uint64_t page_num,
    uint64_t *restrict metadata_start,
    uint64_t *restrict section_bytes);

bool commit_transaction(txn_t *tx) {
  txn_state_t *state = tx->state;
  size_t number_of_buckets = get_number_of_buckets(state);

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_hash_entry_t *entry = &state->entries[i];
    if (!entry->address) continue;

    if (!write_file(state->db->handle, entry->page_num * PAGE_SIZE,
                    entry->address, PAGE_SIZE)) {
      fault(EIO, "Unable to write page_header %lu", entry->page_num);
    }
    free(entry->address);
    entry->address = 0;
  }
  return true;
}

bool close_transaction(txn_t *tx) {
  if (!tx->state) return true;  // probably double close?
  txn_state_t *state = tx->state;
  size_t number_of_buckets = get_number_of_buckets(state);
  bool result = true;
  for (size_t i = 0; i < number_of_buckets; i++) {
    page_hash_entry_t *entry = &state->entries[i];
    if (!entry->address) continue;

    free(entry->address);
    entry->address = 0;
  }

  free(tx->state);

  tx->state = 0;
  return result;
}

bool create_transaction(database_t *db, uint32_t flags, txn_t *tx) {
  assert_no_existing_errors();

  size_t initial_size =
      sizeof(txn_state_t) + sizeof(page_hash_entry_t) * 8;
  txn_state_t *state = calloc(1, initial_size);
  if (!state) {
    fault(ENOMEM, "Unable to allocate memory for transaction state");
  }
  memset(state, 0, initial_size);
  state->allocated_size = initial_size;
  state->flags = flags;
  state->db = db->state;

  tx->state = state;
  return true;
}

static bool lookup_entry_in_tx(txn_state_t *state, uint64_t page_num,
                               page_hash_entry_t **entry) {
  size_t number_of_buckets = get_number_of_buckets(state);
  size_t starting_pos = (size_t)(page_num % number_of_buckets);
  // we use linear probing to find a value in case of collisions
  for (size_t i = 0; i < number_of_buckets; i++) {
    size_t index = (i + starting_pos) % number_of_buckets;
    if (!state->entries[index].address) {
      // empty value, so there is no match
      return false;
    }
    if (state->entries[index].page_num == page_num) {
      *entry = &state->entries[index];
      return true;
    }
  }
  return false;
}

enum hash_resize_status {
  hash_resize_success,
  hash_resize_err_no_mem,
  hash_resize_err_failure,
};

static enum hash_resize_status expand_hash_table(
    txn_state_t **state_ptr, size_t number_of_buckets) {
  size_t new_number_of_buckets = number_of_buckets * 2;
  size_t new_size = sizeof(txn_state_t) + (new_number_of_buckets *
                                           sizeof(page_hash_entry_t));
  txn_state_t *state = *state_ptr;
  txn_state_t *new_state = calloc(1, new_size);
  if (!new_state) {
    // we are OOM, but we'll accept that and let the hash
    // table fill to higher capacity, caller may decide to
    // error
    return hash_resize_err_no_mem;
  }
  memcpy(new_state, state, sizeof(txn_state_t));
  new_state->allocated_size = new_size;

  for (size_t i = 0; i < number_of_buckets; i++) {
    page_hash_entry_t *entry = &state->entries[i];
    if (!entry->address) continue;
    size_t starting_pos = entry->page_num % new_number_of_buckets;
    bool located = false;
    for (size_t j = 0; j < new_number_of_buckets; j++) {
      size_t index = (j + starting_pos) % new_number_of_buckets;
      if (!new_state->entries[index].address) {  // empty
        new_state->entries[index] = state->entries[i];
        located = true;
        break;
      }
    }
    if (!located) {
      push_error(
          EINVAL,
          "Failed to find spot for %lu after hash table resize",
          entry->page_num);
      free(new_state);
      return hash_resize_err_failure;
    }
  }

  *state_ptr = new_state;  // update caller's reference
  free(state);
  return hash_resize_success;
}

static bool allocate_entry_in_tx(txn_state_t **state_ptr,
                                 uint64_t page_num,
                                 page_hash_entry_t **entry) {
  txn_state_t *state = *state_ptr;
  size_t number_of_buckets = get_number_of_buckets(state);
  size_t starting_pos = (size_t)(page_num % number_of_buckets);
  // we use linear probing to find a value in case of collisions
  for (size_t i = 0; i < number_of_buckets; i++) {
    size_t index = (i + starting_pos) % number_of_buckets;
    if (state->entries[index].page_num == page_num &&
        state->entries[index].address) {
      fault(EINVAL,
            "Attempted to allocate entry for page_header %lu "
            "which already exist "
            "in the table",
            page_num);
    }

    if (!state->entries[index].address) {
      size_t max_pages = (number_of_buckets * 3 / 4);
      // check the load factor
      if (state->modified_pages + 1 < max_pages) {
        state->modified_pages++;
        state->entries[index].page_num = page_num;
        *entry = &state->entries[index];
        return true;
      }
      switch (expand_hash_table(state_ptr, number_of_buckets)) {
        case hash_resize_success:
          // try again, now we'll have enough room
          return allocate_entry_in_tx(state_ptr, page_num, entry);
        case hash_resize_err_no_mem:
          // we'll accept it here and just have higher
          // load factor
          break;
        case hash_resize_err_failure:
          push_error(
              EINVAL,
              "Failed to add page_header %lu to the transaction "
              "hash table",
              page_num);
          return false;
      }
    }
  }

  switch (expand_hash_table(state_ptr, number_of_buckets)) {
    case hash_resize_success:
      // try again, now we'll have enough room
      return allocate_entry_in_tx(state_ptr, page_num, entry);
    case hash_resize_err_no_mem:
      // we are at 100% capacity, can't recover, will error now
      fault(ENOMEM,
            "Can't allocate to add page_header %lu to the "
            "transaction hash table",
            page_num);
    case hash_resize_err_failure:
      fault(EINVAL,
            "Failed to add page_header %lu to the transaction "
            "hash table",
            page_num);
  }
}

static MUST_CHECK bool modify_page_metadata(
    txn_t *tx, uint64_t page_num, page_metadata_t **page_metadata) {
  file_header_t *header = &tx->state->db->header;
  page_t page = {.page_num = page_num};
  size_t metadata_index;
  ensure(get_metadata_page_offset(header->number_of_pages, page_num,
                                  &page.page_num, &metadata_index));

  ensure(modify_page(tx, &page));
  *page_metadata = (page_metadata_t *)page.address + metadata_index;
  return true;
}

static MUST_CHECK bool get_page_metadata(
    txn_t *tx, uint64_t page_num, page_metadata_t **page_metadata) {
  file_header_t *header = &tx->state->db->header;
  page_t page = {.page_num = page_num};
  size_t metadata_index;
  ensure(get_metadata_page_offset(header->number_of_pages, page_num,
                                  &page.page_num, &metadata_index));
  page.flags = page_op_skip_metadata;
  ensure(get_page(tx, &page));
  *page_metadata = (page_metadata_t *)page.address + metadata_index;
  return true;
}

bool get_page(txn_t *tx, page_t *page_header) {
  assert_no_existing_errors();

  if (!(page_header->flags & page_op_skip_metadata)) {
    page_metadata_t *metadata;
    ensure(get_page_metadata(tx, page_header->page_num, &metadata));
    page_header->overflow_size = metadata->overflow_size;
  }

  page_hash_entry_t *entry;
  if (lookup_entry_in_tx(tx->state, page_header->page_num, &entry)) {
    page_header->address = entry->address;
    return true;
  }
  uint64_t offset = page_header->page_num * PAGE_SIZE;
  if (offset + PAGE_SIZE > tx->state->db->address_size) {
    fault(ERANGE,
          "Requests page_header %lu is outside of the bounds of "
          "the file (%lu)",
          page_header->page_num, tx->state->db->address_size);
  }

  page_header->address = ((char *)tx->state->db->address + offset);
  return true;
}

bool modify_page(txn_t *tx, page_t *page_header) {
  assert_no_existing_errors();

  page_metadata_t *metadata;
  ensure(get_page_metadata(tx, page_header->page_num, &metadata));

  page_header->overflow_size = metadata->overflow_size;

  page_hash_entry_t *entry;
  if (lookup_entry_in_tx(tx->state, page_header->page_num, &entry)) {
    page_header->address = entry->address;
    return true;
  }

  uint64_t offset = page_header->page_num * PAGE_SIZE;
  if (offset + PAGE_SIZE > tx->state->db->address_size) {
    fault(ERANGE,
          "Requests page_header %lu is outside of the bounds of "
          "the file (%lu)",
          page_header->page_num, tx->state->db->address_size);
  }
  void *original = ((char *)tx->state->db->address + offset);
  void *modified;
  int rc = posix_memalign(&modified, PAGE_ALIGNMENT, PAGE_SIZE);
  if (rc) {
    fault(rc, "Unable to allocate memory for a COW page_header %lu",
          page_header->page_num);
  }
  memcpy(modified, original, PAGE_SIZE);
  if (!allocate_entry_in_tx(&tx->state, page_header->page_num,
                            &entry)) {
    push_error(EINVAL, "Failed to allocate entry");
    free(modified);
    return false;
  }
  entry->address = modified;
  page_header->address = modified;
  return true;
}

static void close_transaction_p(void *p) {
  if (!close_transaction(*(txn_t **)p)) {
    push_error(EINVAL, "Failed to close transaction");
  }
}

static inline uint64_t get_number_of_free_space_bitmap(
    db_state_t *db) {
  return db->header.number_of_pages / BITS_IN_PAGE +
         (db->header.number_of_pages % BITS_IN_PAGE ? 1 : 0);
}

static inline void set_bit(uint64_t *buffer, uint64_t pos) {
  buffer[pos / 64] |= (1UL << pos % 64);
}

static inline void clear_bit(uint64_t *buffer, uint64_t pos) {
  buffer[pos / 64] ^= ~(1UL << pos % 64);
}

static bool mark_page_as_busy(txn_t *restrict tx, uint64_t page_num) {
  file_header_t *header = &tx->state->db->header;
  uint64_t start = header->free_space_bitmap_start;

  uint64_t relevant_free_space_bitmap_page =
      start + page_num / BITS_IN_PAGE;

  page_t bitmap_page = {.page_num = relevant_free_space_bitmap_page};
  if (!modify_page(tx, &bitmap_page)) {
    fault(EINVAL, "Unable to modify free space page %lu",
          relevant_free_space_bitmap_page);
  }
  set_bit(bitmap_page.address, page_num % BITS_IN_PAGE);
  return true;
}

bool allocate_page(txn_t *restrict tx, page_t *restrict page,
                   uint64_t near_page) {
  uint32_t required_size = 1;
  if (page->overflow_size > 0) {
    required_size = page->overflow_size / PAGE_SIZE +
                    (page->overflow_size % PAGE_SIZE ? 1 : 0);
  }

  file_header_t *header = &tx->state->db->header;
  uint64_t start = header->free_space_bitmap_start;
  uint64_t count = header->free_space_bitmap_in_pages;

  for (uint64_t i = 0; i < count; i++) {
    page_t bitmap_page = {.page_num = i + start};
    ensure(get_page(tx, &bitmap_page));

    size_t pos;
    if (find_free_range_in_bitmap(bitmap_page.address,
                                  PAGE_SIZE / sizeof(uint64_t),
                                  required_size, near_page, &pos)) {
      page->page_num = pos;
      ensure(modify_page(tx, page));
      memset(page->address, 0, PAGE_SIZE);
      // now need to mark it as busy in the bitmap
      for (uint32_t j = 0; j < required_size; j++) {
        ensure(mark_page_as_busy(tx, pos + j));

        page_metadata_t *metadata;
        ensure(modify_page_metadata(tx, pos + j, &metadata));
        memset(metadata, 0, sizeof(page_metadata_t));
        if (required_size == 1) {
          metadata->flags = page_single;
        } else if (j == 0) {
          metadata->flags = page_overflow_first;
          metadata->overflow_size = page->overflow_size;
        } else {
          metadata->flags = page_overflow_rest;
          metadata->overflow_size =
              page->overflow_size - j * PAGE_SIZE;
        }
      }
      return true;
    }
  }
  push_error(ENOSPC,
             "No more room left in the file %s to allocate %d",
             get_file_name(tx->state->db->handle), PAGE_SIZE);
  return false;
}

bool free_page(txn_t *tx, page_t *page) {
  page_metadata_t *metadata;
  ensure(modify_page_metadata(tx, page->page_num, &metadata));
  memset(metadata, 0, sizeof(page_metadata_t));

  ensure(modify_page(tx, page));

  memset(page->address, 0, PAGE_SIZE);

  page_t free_space_page = {.page_num =
                                page->page_num / BITS_IN_PAGE};
  ensure(modify_page(tx, &free_space_page));

  clear_bit(free_space_page.address, page->page_num % BITS_IN_PAGE);

  return true;
}

static MUST_CHECK bool initialize_file_structure(database_t *db) {
  txn_t tx;
  ensure(create_transaction(db, 0, &tx));
  defer(close_transaction_p, &tx);

  file_header_t *header = &db->state->header;

  header->free_space_bitmap_start = 1;
  header->free_space_bitmap_in_pages =
      get_number_of_free_space_bitmap(db->state);

  page_t page_bitmap = {.page_num = 1};
  ensure(modify_page(&tx, &page_bitmap));

  uint64_t busy_pages =
      db->state->header.free_space_bitmap_in_pages + 1;
  // TODO: handle initial file allocation that is greater than 32TB
  fail(busy_pages > BITS_IN_PAGE, ENOMEM,
       "Free space bitmap metadata cannot exceed %d entries",
       BITS_IN_PAGE);

  for (size_t i = 0; i < busy_pages; i++) {
    set_bit(page_bitmap.address, i);
  }

  page_t page_header = {.page_num = 0};
  ensure(modify_page(&tx, &page_header));

  header->pages_in_metadata_section = PAGES_IN_METADATA_PAGE;

  page_t meta_page;
  for (size_t i = 0; i < busy_pages; i++) {
    page_metadata_t *metadata;
    ensure(modify_page_metadata(&tx, i, &metadata));
    memset(metadata, 0, sizeof(page_metadata_t));

    metadata->flags = page_single | page_metadata;
  }

  for (size_t i = 0; i < header->number_of_pages;
       i += PAGES_IN_METADATA_PAGE) {
    uint64_t section_pages;
    get_metadata_section_stats(header->number_of_pages, i,
                               &meta_page.page_num, &section_pages);
    uint64_t base_page_num = meta_page.page_num;
    for (size_t j = 0; j < section_pages; j++) {
      page_metadata_t *metadata;
      ensure(modify_page_metadata(&tx, base_page_num + j, &metadata));
      memset(metadata, 0, sizeof(page_metadata_t));
      metadata->flags = page_single | page_metadata;
    }
  }

  memcpy(page_header.address, &db->state->header,
         sizeof(file_header_t));
  memcpy((char *)page_header.address + PAGE_SIZE / 2,
         &db->state->header, sizeof(file_header_t));

  ensure(commit_transaction(&tx));

  return true;
}

static MUST_CHECK bool handle_newly_opened_database(database_t *db) {
  db_state_t *state = db->state;
  file_header_t *header1 = state->address;
  file_header_t *header2 =
      (void *)((char *)state->address + PAGE_SIZE / 2);

  // at this point state->header is zeroed
  // if both headers are zeroed, we are probably dealing with a new
  // database
  bool isNew =
      !memcmp(header1, &state->header, sizeof(file_header_t)) &&
      !memcmp(header2, &state->header, sizeof(file_header_t));

  if (isNew) {
    // now needs to set it up
    state->header.magic = FILE_HEADER_MAGIC;
    state->header.version = 1;
    state->header.page_size = PAGE_SIZE;
    state->header.number_of_pages = state->address_size / PAGE_SIZE;
    ensure(initialize_file_structure(db));
  }
  file_header_t *selected =
      header1->magic != FILE_HEADER_MAGIC ? header2 : header1;

  fail(selected->magic != FILE_HEADER_MAGIC, EINVAL,
       "Unable to find matching header in first page_header of %s",
       get_file_name(state->handle));

  fail(selected->pages_in_metadata_section != PAGES_IN_METADATA_PAGE,
       EINVAL,
       "The number of pages in the metadata section is %d but was "
       "expecting %d",
       selected->pages_in_metadata_section, PAGES_IN_METADATA_PAGE);

  fail(selected->number_of_pages * PAGE_SIZE > state->address_size,
       EINVAL,
       "The size of the file %s is %lu but expected to have %lu "
       "pages, file was probably truncated",
       get_file_name(state->handle), state->address_size,
       selected->number_of_pages * PAGE_SIZE);

  fail(selected->page_size != PAGE_SIZE, EINVAL,
       "File %s page_header size is %d, expected %d",
       get_file_name(state->handle), selected->page_size, PAGE_SIZE);

  return true;
}

static MUST_CHECK bool close_database_state(db_state_t *state) {
  if (!state) return true;  // double close?
  bool result = true;
  if (state->address) {
    if (unmap_file(state->address, state->address_size)) {
      push_error(EINVAL, "Failed to unmap file");
      result = false;
    }
  }
  if (!close_file(state->handle)) {
    push_error(EINVAL, "Failed to close file");
    result = false;
  }
  free(state);
  return result;
}

static void close_database_state_p(void *p) {
  if (!close_database_state(p)) {
    push_error(EINVAL, "Failed to close database");
  }
}

bool close_database(database_t *db) {
  if (!db->state) return true;  // double close?
  bool result = close_database_state(db->state);
  db->state = 0;
  return result;
}

static MUST_CHECK bool validate_options(database_options_t *options) {
  if (!options->minimum_size) options->minimum_size = 128 * 1024;

  if (options->minimum_size < 128 * 1024) {
    push_error(EINVAL, "The minimum_size cannot be less than %d",
               128 * 1024);
    return false;
  }

  if (options->minimum_size % PAGE_SIZE) {
    push_error(EINVAL,
               "The minimum size must be page_header aligned (%d)",
               PAGE_SIZE);
    return false;
  }

  return true;
}

bool open_database(const char *path, database_options_t *options,
                   database_t *db) {
  db_state_t *ptr = 0;
  // defer is called on the _pointer_ of
  // ptr, not its value;
  try_defer(close_database_state_p, &ptr, open_db_successful);

  ensure(validate_options(options));

  size_t db_state_size;
  ensure(get_file_handle_size(path, &db_state_size));
  db_state_size += sizeof(db_state_t);

  ptr = calloc(1, db_state_size);
  if (!ptr) {
    fault(ENOMEM,
          "Unable to allocate "
          "database state struct: %s",
          path);
  }
  ptr->handle = (file_handle_t *)(ptr + 1);

  if (!create_file(path, ptr->handle)) {
    fault(EIO, "Unable to create file for %s", path);
  }

  ptr->options = *options;
  ensure(
      ensure_file_minimum_size(ptr->handle, options->minimum_size));

  ensure(get_file_size(ptr->handle, &ptr->address_size));

  ensure(map_file(ptr->handle, 0, ptr->address_size, &ptr->address));

  db->state = ptr;

  ensure(handle_newly_opened_database(db));

  open_db_successful = 1;  // ensuring that defer won't clean it
  return true;
}

static inline void get_metadata_section_stats(
    uint64_t total_number_of_pages, uint64_t page_num,
    uint64_t *restrict metadata_start,
    uint64_t *restrict section_pages) {
  uint64_t section_size_bytes, metadata_page_start;
  uint64_t metadata_page_end =
      page_num +
      (PAGES_IN_METADATA_PAGE - (page_num % PAGES_IN_METADATA_PAGE));
  if (metadata_page_end < total_number_of_pages) {
    // full metadata section
    section_size_bytes =
        (PAGES_IN_METADATA_PAGE * sizeof(page_metadata_t));
    metadata_page_start = metadata_page_end;
  } else {
    // past the end of the file, so we need to compute the size of the
    // remainder
    const uint64_t remaining_pages_for_last_section =
        total_number_of_pages % PAGES_IN_METADATA_PAGE;
    section_size_bytes =
        (remaining_pages_for_last_section * sizeof(page_metadata_t));
    metadata_page_start = total_number_of_pages;
  }

  const uint64_t section_size_pages =
      section_size_bytes / PAGE_SIZE +
      (section_size_bytes % PAGE_SIZE ? 1 : 0);
  metadata_page_start -= section_size_pages;

  *section_pages = section_size_pages;
  *metadata_start = metadata_page_start;
}

MUST_CHECK bool get_metadata_page_offset(
    uint64_t total_number_of_pages, uint64_t page_num,
    uint64_t *metadata_page_num, size_t *metadata_index) {
  if (page_num >= total_number_of_pages) {
    push_error(
        ERANGE,
        "Requested page %lu is outside of the file limits: %lu",
        page_num, total_number_of_pages);
    return false;
  }

  uint64_t metadata_page_start;
  uint64_t section_size_bytes;
  get_metadata_section_stats(total_number_of_pages, page_num,
                             &metadata_page_start,
                             &section_size_bytes);

  const size_t index_of_metadata_in_section =
      page_num % PAGES_IN_METADATA_PAGE;

  const size_t index_of_page_in_section =
      index_of_metadata_in_section /
      (PAGE_SIZE / sizeof(page_metadata_t));
  *metadata_page_num = metadata_page_start + index_of_page_in_section;
  *metadata_index = index_of_metadata_in_section %
                    (PAGE_SIZE / sizeof(page_metadata_t));
  return true;
}
