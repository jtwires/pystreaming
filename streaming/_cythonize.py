import inspect
import functools

def cythonize(py):
    """substitutes a cython version when available"""
    d = dict(inspect.getmembers(inspect.stack()[1][0]))['f_globals']
    c = d.get('_{}'.format(py.__name__))

    if inspect.isclass(py):
        class MetaClass(type):
            def __call__(cls, *args, **kwargs):
                if kwargs.pop('python', False) or not c:
                    return super(MetaClass, cls).__call__(*args, **kwargs)
                return c(*args, **kwargs)

        class Decorated(py):
            __metaclass__ = MetaClass

        Decorated.__name__ = py.__name__
        Decorated.__module__ = py.__module__
        Decorated.__doc__ = py.__doc__

        return Decorated
    else:
        @functools.wraps(py)
        def decorated(*args, **kwargs):
            if kwargs.pop('python', False) or not c:
                return py(*args, **kwargs)
            return c(*args, **kwargs)
        return decorated
