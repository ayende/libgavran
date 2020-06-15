#pragma once

typedef struct datatabase_state db_state_t;

typedef struct transaction_state tx_state_t;

typedef struct database {
  db_state_t* state;
} db_t;

typedef struct transaction {
  tx_state_t* state;
} txn_t;