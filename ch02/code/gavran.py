import os
import errno
from ctypes import *

gvn = cdll.LoadLibrary("./build/gavran.so")  

methods = [
    ("errors_get_count", [], c_size_t),
    ("errors_get_codes", [POINTER(c_size_t)], POINTER(c_int)),
    ("errors_get_messages", [POINTER(c_size_t)], POINTER(c_char_p)),
    ("errors_clear", [], None),
    ("errors_print_all", [], None),
    ("errors_append_message", [c_char_p] , c_void_p),
    ("errors_push_new", [c_char_p,c_int, c_char_p, c_int, c_char_p], c_void_p)
]

for method in methods:
    m = getattr(gvn, method[0])
    setattr(m, "argtypes", method[1])
    setattr(m, "restype", method[2])

class Errors:

    @staticmethod
    def print_all():
        gvn.errors_print_all()

    @staticmethod
    def get_count():
        return gvn.errors_get_count()
    
    @staticmethod
    def clear():
            gvn.errors_clear()

    @staticmethod
    def get_errors():
        size = c_size_t()
        codes_ptr = gvn.errors_get_codes(pointer(size))
        msgs_ptr = gvn.errors_get_messages(pointer(size))
        return [(codes_ptr[i], None if msgs_ptr[i] is None else str(msgs_ptr[i])) for i in range(size.value)]

    @staticmethod
    def get_codes():
        size = c_size_t()
        codes_ptr = gvn.errors_get_codes(pointer(size))
        codes = [codes_ptr[i].value for i in range(size.value)]
        return codes

    @staticmethod
    def get_messages():
        size = c_size_t()
        msgs_ptr = gvn.errors_get_messages(pointer(size))
        msgs = [str(msgs_ptr[i]) for i in range(size.value)]
        return msgs

    @staticmethod
    def push(file, line, func, code, format):
        gvn.errors_push_new(file, line, func, code, format)

    @staticmethod
    def append(format):
        gvn.errors_append_message(format)
