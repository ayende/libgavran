def load_api():
    from importlib.machinery import SourceFileLoader
    loader = SourceFileLoader("gavran", "./../../pyapi/api.py")
    mod = loader.load_module()
    loader.exec_module(mod)


load_api()
