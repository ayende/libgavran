from gavran import *

import uuid
import pytest
import ctypes
import os
import os.path

from bitstring import ConstBitStream

def setup_function(function):
    Errors.clear()
    global path 
    path = "/tmp/" +  uuid.uuid1().hex


def test_closing_file_does_not_commit_pending_transactions():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        rtx = db.read_txn() 
        rtx.abandon()#intentionally leaking this
        with db.write_txn() as wtx:
            for i in range(2,4):
                p = wtx.modify(i)
                b = b'Hello Garvan Python'
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()


    s = ConstBitStream(filename=path)
    assert not s.find( b'Hello Garvan Python', bytealigned=True)
    
    
def test_reopen_file_recover_committed_transactions():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        rtx = db.read_txn() 
        #intentionally leaking this, prevent writing to data file
        rtx.abandon()
        with db.write_txn() as wtx:
            for i in range(2,6):
                p = wtx.modify(i)
                b = b'Hello Garvan Python'
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()

    # should recover and apply committed tx
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.read_txn() as rtx2:
            for i in range(2,6):
                p2 = rtx2.get(i)
                data =ctypes.string_at(p2.address)
                assert b'Hello Garvan Python' == data


def test_closing_file_with_all_tx_committed_means_no_recovery():
    last_write_pos = 0
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.write_txn() as wtx:
            for i in range(2,4):
                p = wtx.modify(i)
                b = b'Hello Garvan Python'
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()
        last_write_pos = db.test_wal_last_pos()

    with Database(path,  DatabaseOptions(128*1024)) as db:
        assert last_write_pos == db.test_wal_last_pos()
  