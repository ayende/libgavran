#include <gavran/db.h>
#include <gavran/internal.h>

// tag::db_initialize_default_read_tx[]
implementation_detail result_t
db_initialize_default_read_tx(db_state_t *db_state) {
  ensure(mem_calloc((void *)&db_state->default_read_tx,
                    sizeof(txn_state_t)));

  db_state->default_read_tx->modified_pages = 0;
  db_state->default_read_tx->db = db_state;
  db_state->default_read_tx->global_state = db_state->global_state;
  // only the default read tx has this setup
  db_state->default_read_tx->flags = TX_READ | TX_COMMITED;
  db_state->default_read_tx->can_free_after_tx_id = UINT64_MAX;
  db_state->last_write_tx = db_state->default_read_tx;
  return success();
}
// end::db_initialize_default_read_tx[]
