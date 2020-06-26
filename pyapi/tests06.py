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

def test_can_allocate__multiple_pages():
    with Database(path,  DatabaseOptions(128*1024)) as db:
        with db.txn(flags =2) as tx:
            p = tx.allocate(size = 8192*4)
            assert 2 == p.page_num
            p = tx.allocate(size = 8192)
            assert 6 == p.page_num

def test_will_not_allocate_on_metadata_boundary_small():
    with Database(path,  DatabaseOptions(4*1028*1024)) as db:
        with db.txn(flags =2) as tx:
            p = tx.allocate(size = 8192*96)
            assert 2 == p.page_num
            p = tx.allocate(size = 8192* 32)
            assert 129 == p.page_num

def test_can_allocate_very_large_values():
    with Database(path,  DatabaseOptions(4*1028*1024)) as db:
        with db.txn(flags =2) as tx:
            p = tx.allocate(size = 8192*268)
            assert 116 == p.page_num
            p = tx.allocate(size = 8192* 32)
            assert 2 == p.page_num

def test_after_move_to_next_range_will_still_use_existing_space():
    with Database(path,  DatabaseOptions(4*1028*1024)) as db:
        with db.txn(flags =2) as tx:
            p = tx.allocate(size = 8192*96)
            assert 2 == p.page_num
            p = tx.allocate(size = 8192* 32)
            assert 129 == p.page_num
            p = tx.allocate(size = 8192* 16)
            assert 98 == p.page_num

def test_can_free_and_reuse():
    with Database(path,  DatabaseOptions(4*1028*1024)) as db:
        with db.txn(flags =2) as tx:
            p = tx.allocate(size = 8192*96)
            assert 2 == p.page_num
            p = tx.allocate(size = 8192* 32)
            assert 129 == p.page_num
            tx.free(p)
            p = tx.allocate(size = 8192* 268)
            assert 116 == p.page_num
