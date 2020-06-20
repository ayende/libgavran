from gavran import *

def setup_function(function):
    Errors.clear()

def test_no_errors_should_return_zero_count():
    assert Errors.get_count() == 0

def test_can_get_error_recorded():
    Errors.push(b"code.c", 12, b"run", 22)
    assert Errors.get_count() == 1
    assert "code.c" in Errors.get_messages()[0]

def test_can_append_to_same_error():
    Errors.push(b"code.c", 12, b"run", 22)
    Errors.append(b", with i = 2")
    assert Errors.get_count() == 1
    assert "with i = 2" in Errors.get_messages()[0]

def test_will_resolve_error_codes():
    Errors.push(b"code.c", 12, b"run", 22)
    assert Errors.get_count() == 1
    assert "Invalid argument" in Errors.get_messages()[0]

def test_push_multiple_errors():
    Errors.push(b"code.c", 12, b"run", 22)
    Errors.append(b"Opps")
    Errors.push(b"code.c", 13, b"call", 22)
    Errors.append(b"um...")
    assert Errors.get_count() == 2
    assert "run" in Errors.get_messages()[0]
    assert "Opps" in Errors.get_messages()[0]
    assert "call" in Errors.get_messages()[1]
    assert "um..." in Errors.get_messages()[1]

def test_max_errors_is_64():
    for i in range(100):
        Errors.push(b"code.c", i, b"run", 22)

    assert Errors.get_count() == 64
    
def test_very_large_errors_wont_overflow():
    for i in range(100):
        Errors.push(b"code.c", i, b"run", 22)
        Errors.append(b"x" * 256)

    assert Errors.get_count() == 64
    
    errs = Errors.get_errors()
    for i in range(6):
        assert errs[i][0] == 22
        assert len(errs[i][1]) > 0

    for i in range(6, 64):
        assert errs[i][0] == 22   #we retain just the code
        assert errs[i][1] == None # no space for this, so we ignored the message
