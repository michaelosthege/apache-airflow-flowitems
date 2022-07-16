import os

import pytest

from . import decorators


class OnPurposeException(Exception):
    pass


@pytest.mark.parametrize("error", [True, False])
def test_in_tempdir(error):
    @decorators.in_tempdir
    def fun(original: str):
        assert os.getcwd() != original
        if error:
            raise OnPurposeException()
        return

    original = os.getcwd()
    try:
        fun(original)
    except OnPurposeException:
        pass
    assert os.getcwd() == original
    pass
