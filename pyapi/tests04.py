from gavran import *

import uuid
import pytest
import ctypes
import os
import os.path

path = "/tmp/" +  uuid.uuid1().hex

def setup_function(function):
    Errors.clear()
    if os.path.isfile(path):
        os.remove(path)
    if os.path.isdir(path):
        os.rmdir(path)

options = DatabaseOptions()
options.minimum_size = 128*1024

def test_can_create_db_and_Tx():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            pass

def test_can_write_data():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            p = tx.modify(0)
            b = b'Hello Garvan Python'
            ctypes.memmove(p.address, b, len(b))

            tx.commit()

        with db.txn(flags = 0) as tx:
            p = tx.get(0)
            b = b'Hello Garvan Python'
            
            assert b == ctypes.string_at(p.address)

def test_tx_without_commit_change_no_data():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            p = tx.modify(3)
            b = b'Hello Garvan Python'
            ctypes.memmove(p.address, b, len(b))

            # tx.commit() - not doing this

        with db.txn(flags = 0) as tx:
            p = tx.get(3)
            b = b''
            
            assert b == ctypes.string_at(p.address)

def test_value_persisted_across_restarts():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            p = tx.modify(3)
            b = b'Hello Garvan Python'
            ctypes.memmove(p.address, b, len(b))

            tx.commit()

    # create it again

    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            p = tx.get(3)
            b = b'Hello Garvan Python'
               
            assert b == ctypes.string_at(p.address)