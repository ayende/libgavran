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

def test_can_allocate_pages():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            p = tx.allocate()
            assert 2 == p.page_num
            p = tx.allocate()
            assert 3 == p.page_num

def test_can_allocate_till_runs_out():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            for i in range(14):
                p = tx.allocate()
                
            with pytest.raises(GavranError) as g:
                tx.allocate()
            
            assert "No space left on device" in str(g.value)

def test_can_allocate_then_free_then_allocate():
    with Database(path, options) as db:
        with db.txn(flags = 0) as tx:
            p = tx.allocate()
            num = p.page_num
            tx.allocate() #ignored
            tx.free(p)
            p = tx.allocate()
            assert num == p.page_num
