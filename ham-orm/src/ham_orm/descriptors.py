from functools import wraps

class dualmethod:
    def __init__(self, func):
        self.func = func
        wraps(func)(self)

    def __get__(self, obj, objtype=None):
        target_type = objtype if obj is None else type(obj)

        @wraps(self.func)
        def wrapper(*args, **kwargs):
            # If accessed via class, create a fresh instance; else use the instance
            self_obj = obj if obj is not None else target_type()
            return self.func(self_obj, *args, **kwargs)

        return wrapper