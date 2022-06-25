import functools
import os
import tempfile
from typing import Callable


def in_tempdir(func: Callable):
    """Decorates a function such that the function is executed in a temporary directory.

    The temporary directory is set as the current working directory before invokation of the function.
    It is removed and the original CWD is restored after the function returns/crashes.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        orig_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                return func(*args, **kwargs)
            finally:
                os.chdir(orig_cwd)

    return wrapper
