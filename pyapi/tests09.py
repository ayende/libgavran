from gavran import *

import secrets
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

def assert_empty():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.read_txn() as rtx2:
            for i in range(2,6):
                p2 = rtx2.get(i)
                data =ctypes.string_at(p2.address)
                assert b'' == data

def write_to_wal_only():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        rtx = db.read_txn() 
        rtx.abandon()#intentionally leaking this
        with db.write_txn() as wtx:
            for i in range(2,4):
                p = wtx.modify(i)
                b = b'Hello Garvan Python'
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()
            return db.test_wal_last_pos()

def test_corruption_of_wal_will_not_apply_tx():
    last_write_pos = write_to_wal_only()

    with open(path + ".wal","wb") as f:
        f.seek(last_write_pos - 12)
        f.write(b'abc')

    assert_empty()

def test_wal_truncation_will_cause_tx_to_be_skipped():
    last_write_pos = write_to_wal_only()

    with open(path + ".wal","wb") as f:
        f.truncate(last_write_pos - 12)

    assert_empty()
    
def test_will_compress_transactions():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        initial_pos = db.test_wal_last_pos()  
        with db.write_txn() as wtx:
            for i in range(2,9):
                p = wtx.modify(i)
                b = b'x' * 8192
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()
            final_pos = db.test_wal_last_pos()  
            # data was compressed to log, but we have 
            # minimum 8KB writes
            assert (final_pos - initial_pos) <= 8192

def test_will_diff_writes():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.write_txn() as wtx:
            for i in range(2,9):
                p = wtx.modify(i)
                # data is random, can't be meaingfully compressed
                b = secrets.token_bytes(8192)
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()

        initial_pos = db.test_wal_last_pos()  
        with db.write_txn() as wtx:
            for i in range(2,9):
                p = wtx.modify(i)
                b = b'Hello Gavran modified'
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()

            final_pos = db.test_wal_last_pos()  
            # if we didn't diff the data, the random values
            # will ensure that we can't compress well, so we
            # test that diff works in this manner
            assert (final_pos - initial_pos) <= 8192
            
    