import pytest

from main import mytransform

@pytest.mark.parametrize("input_ts,expected",[(1726329273,'2024-09-14'),(1726366892,'2024-09-14')])
def test_date_format_change(input_ts,expected):
    object=mytransform([])

    assert object.date_format_change(input_ts) == expected




