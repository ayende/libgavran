from gavran import *

import uuid
import pytest
import ctypes
import os
import os.path

def setup_function(function):
    Errors.clear()
    global path 
    path = "/tmp/" +  uuid.uuid1().hex


def test_can_get_file_name():   
    with PalFS(path) as file:
        assert path == file.name()

def test_can_close_file():   
    with PalFS(path) as file:
        file.close()

def test_can_create_file():   
    with PalFS(path) as f:
        assert os.path.isfile(path)
        assert os.path.getsize(path) == 0

def test_can_set_file_size():
    with PalFS(path) as file:
        assert os.path.getsize(path) == 0
        file.set_size(1024)
        assert os.path.getsize(path) == 1024
        assert file.size() == 1024

def test_can_write_and_read_memory():
    with PalFS(path) as file:
        file.set_size(1024)
        file.write(0, b'Hello Gavran\0')
        with file.map(0, 1024) as ptr:
            s = ctypes.string_at(ptr).decode('utf-8')
            assert s == "Hello Gavran"

def test_will_get_error_on_opening_a_dir():
    os.mkdir(path)
    with pytest.raises(GavranError) as g:
        with PalFS(path) as file:
            pass
    assert "Is a directory" in str(g.value)


         
