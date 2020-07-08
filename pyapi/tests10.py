from gavran import *
import glob
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

def fill_wal(db):
    # the WAL max size is 16K, each tx is 8K
    for _ in range(3):
        with db.write_txn() as tx:
            for i in range(14):
                p = tx.allocate()
            tx.allocate()
            tx.commit()

def test_can_allocate_and_grow_the_data_file():
    with Database(path, DatabaseOptions(128*1024, 0,  2 * 8192)) as db:
        old_size = os.path.getsize(path)
        fill_wal(db)
        new_size = os.path.getsize(path)
        assert old_size < new_size

def test_wal_stay_within_limit_if_possible():
    with Database(path, DatabaseOptions(128*1024, 0,  2 * 8192)) as db:
        old_size = os.path.getsize(path + "-a.wal")
        fill_wal(db)
        new_size = os.path.getsize(path + "-a.wal")
        assert old_size == new_size


def test_can_grow_the_wal_size():
    with Database(path, DatabaseOptions(128*1024, 0,  2 * 8192)) as db:
        old_size = os.path.getsize(path + "-a.wal")
        db.read_txn().abandon()
        fill_wal(db)
        new_size = os.path.getsize(path + "-a.wal")
        
        assert old_size < new_size

def test_will_use_both_wal_files_and_reset_the_size():
    with Database(path, DatabaseOptions(128*1024, 0,  2 * 8192)) as db:
        old_size_a = os.path.getsize(path + "-a.wal")
        rtx1 = db.read_txn()
        fill_wal(db)
        # can't checkpoint due to rtx1, so will grow
        new_size_a = os.path.getsize(path + "-a.wal")
        assert old_size_a < new_size_a

        # now we have a new transaction prevent complete clear
        rtx2 = db.read_txn()
        rtx1.close() # reason to stop checkpointing A is gone
        
        # should now switch to B
        ols_size_b = os.path.getsize(path+"-b.wal")
        fill_wal(db)
        new_size_b = os.path.getsize(path+"-b.wal")
        
        assert ols_size_b < new_size_b

        rtx2.close() # can now switch back to A
        # when this happens, we reset A
        updated_size_a = os.path.getsize(path+"-a.wal")
        assert updated_size_a < new_size_a



        

