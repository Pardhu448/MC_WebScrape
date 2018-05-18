import hashlib
from functools import wraps
import os, pickle

def arg_to_key(*args):
    
    assert args, 'There should be atleast one argument for key'
    args_str = ('_').join(map(str, [each_kwarg for each_kwarg in args]))
    key = hashlib.sha224(args_str.encode('utf-8')).hexdigest()
    #key = haslib.sha224(b str(kwargs['title']))
    return key

def memoize(datapath):
    def memoize_actual(func): 
        if os.path.exists(datapath):
            with open(datapath, 'rb') as f:
                cache = pickle.load(f)
        else:
            cache = {}
        
        @wraps(func)
        def wrap(*args, **kwargs):
            key = arg_to_key(*args)
            if key not in cache:
                cache[key] = func(*args, **kwargs)
                # update the cache file
                with open(datapath, 'wb') as f:
                    pickle.dump(cache, f)
            return cache[key]
        return wrap
    return memoize_actual






