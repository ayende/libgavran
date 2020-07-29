#include <gavran/db.h>
#include <gavran/internal.h>

// tag::db_initialize_default_read_tx[]
implementation_detail result_t
db_initialize_default_read_tx(db_state_t *db_state) {
  ensure(mem_calloc((void *)&db_state->default_read_tx,
                    sizeof(txn_state_t)));
  txn_state_t *tx = db_state->default_read_tx;
  tx->modified_pages = 0;
  tx->db = db_state;
  tx->map = db_state->map;
  tx->number_of_pages = db_state->number_of_pages;
  tx->flags = TX_READ | TX_COMMITED | db_state->options.flags;
  tx->can_free_after_tx_id = UINT64_MAX;
  db_state->last_write_tx = db_state->default_read_tx;
  return success();
}
// end::db_initialize_default_read_tx[]
