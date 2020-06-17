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