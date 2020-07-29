#include <gavran/db.h>
#include <gavran/internal.h>
#include <string.h>

// tag::txn_create[]
result_t txn_create(db_t *db, uint32_t flags, txn_t *tx) {
  errors_assert_empty();

  size_t cancel_defer = 0;
  txn_state_t *state;
  ensure(mem_calloc((void *)&state, sizeof(txn_state_t)));
  try_defer(free, state, cancel_defer);

  ensure(hash_new(8, &state->modified_pages));

  state->flags = flags;
  state->db = db->state;
  state->map = db->state->map;
  state->number_of_pages = db->state->number_of_pages;

  tx->state = state;
  cancel_defer = 1;
  return success();
}
// end::txn_create[]

// tag::txn_raw_get_page[]
result_t txn_raw_get_page(txn_t *tx, page_t *page) {
  errors_assert_empty();
  page->address = 0;
  if (hash_lookup(tx->state->modified_pages, page)) return success();

  if (!page->address) {
    if (!page->number_of_pages) page->number_of_pages = 1;
    ensure(pages_get(tx, page));
  }

  return success();
}
// end::txn_raw_get_page[]

// tag::txn_raw_modify_page[]
result_t txn_raw_modify_page(txn_t *tx, page_t *page) {
  errors_assert_empty();
  if (hash_lookup(tx->state->modified_pages, page)) {
    return success();
  }

  page_t original = {.page_num = page->page_num};
  ensure(txn_raw_get_page(tx, &original));
  if (!page->number_of_pages) page->number_of_pages = 1;
  size_t done = 0;
  ensure(mem_alloc_page_aligned(&page->address,
                                PAGE_SIZE * page->number_of_pages));
  try_defer(free, page->address, done);

  memcpy(page->address, original.address,
         (PAGE_SIZE * page->number_of_pages));
  page->previous = original.address;
  ensure(hash_put_new(&tx->state->modified_pages, page),
         msg("Failed to allocate entry"));
  done = 1;
  return success();
}
// end::txn_raw_modify_page[]

// tag::txn_commit[]
result_t txn_commit(txn_t *tx) {
  errors_assert_empty();

  size_t iter_state = 0;
  page_t *p;
  while (hash_get_next(tx->state->modified_pages, &iter_state, &p)) {
    ensure(pages_write(tx->state->db, p));
  }

  return success();
}
// end::txn_commit[]

// tag::txn_close[]
result_t txn_close(txn_t *tx) {
  if (!tx || !tx->state) return success();

  size_t iter_state = 0;
  page_t *p;
  while (hash_get_next(tx->state->modified_pages, &iter_state, &p)) {
    free(p->address);
    p->address = 0;
  }
  free(tx->state->modified_pages);
  free(tx->state);

  tx->state = 0;
  return success();
}
// end::txn_close[]
