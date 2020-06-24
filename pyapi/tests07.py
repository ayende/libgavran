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

def test_can_not_see_changes_from_tx_after_me():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.read_txn() as rtx:
            p2 = rtx.get(2)
            assert b'' == ctypes.string_at(p2.address)

            with db.write_txn() as wtx:
                for i in range(2,4):
                    p = wtx.modify(i)
                    b = b'Hello Garvan Python'
                    ctypes.memmove(p.address, b, len(b))
                wtx.commit()

            # again, after the tx commit
            assert b'' == ctypes.string_at(p2.address)
            # a page we get after the tx commit
            p3 = rtx.get(3)
            assert b'' == ctypes.string_at(p3.address)

        # new tx can see it all
        with db.read_txn() as rtx2:
            for i in range(2,4):
                p2 = rtx2.get(i)
                data =ctypes.string_at(p2.address)
                assert b'Hello Garvan Python' == data

def test_will_write_to_disk_if_no_read_tx():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.write_txn() as wtx:
            for i in range(2,4):
                p = wtx.modify(i)
                b = b'Hello Garvan Python'
                ctypes.memmove(p.address, b, len(b))
            wtx.commit()

        # here we read directly from the mmap, test only
        # function!
        address = db.test_get_map_at(2)
        data =ctypes.string_at(address)
        assert b'Hello Garvan Python' == data
        
def test_will_not_write_to_disk_with_read_tx():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        rtx = None
        with db.write_txn() as wtx:
            for i in range(2,4):
                p = wtx.modify(i)
                b = b'Hello Garvan Python'
                ctypes.memmove(p.address, b, len(b))
            rtx = db.read_txn()
            wtx.commit()

        # here we read directly from the mmap, test only
        # function!
        address = db.test_get_map_at(2)
        assert b'' == ctypes.string_at(address)
        rtx.close() # not it should write it
        assert b'Hello Garvan Python' == ctypes.string_at(address)

def test_interleaved_transactions():        
    with Database(path,  DatabaseOptions(128*1024)) as db:
        assert_raw(db, 0) # initial state

        rtx1 = write_and_return_rtx(db, 1)
        rtx2 = write_and_return_rtx(db, 2)
        rtx3 = write_and_return_rtx(db, 3)
        rtx4 = write_and_return_rtx(db, 4)
        rtx5 = write_and_return_rtx(db, 5)
        
        assert_raw(db, 0) # no writes

        rtx1.close() 
        assert_raw(db, 1)
        rtx2.close()
        assert_raw(db, 2) # written as expected

        rtx6 = write_and_return_rtx(db, 6)

        rtx4.close() # reverse order!
        assert_raw(db, 2) # no change
        rtx3.close()
        assert_raw(db, 4) # only write the end

        rtx7 = write_and_return_rtx(db, 7)
        rtx8 = write_and_return_rtx(db, 8)

        rtx5.close()
        rtx8.close()
        rtx6.close()
        assert_raw(db, 6) # midway
        rtx7.close()
        assert_raw(db, 8) # to the end

def write_and_return_rtx(db, n):
    with db.write_txn() as wtx:
        p = wtx.modify(2)
        b = str(n).zfill(2).encode('utf-8')
        ctypes.memmove(p.address, b, len(b))
        wtx.commit()
        return db.read_txn()

def assert_raw(db, expected):
    b = str(expected).zfill(2).encode('utf-8')
    if expected == 0:
        b = b''

    address = db.test_get_map_at(2)
    assert b == ctypes.string_at(address)
