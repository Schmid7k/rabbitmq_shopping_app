def xprint(*args, **kwargs):
    print("LOG: " + " ".join(map(str, args)), **kwargs)
