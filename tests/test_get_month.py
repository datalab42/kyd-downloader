
import sys
sys.path.append('../functions/')
from datetime import date
from kyd.data.downloaders import get_month


def test_get_month():
    assert get_month(date(2020, 7, 4), 0) == date(2020, 7, 1)
    assert get_month(date(2020, 7, 4), -1) == date(2020, 6, 1)
    assert get_month(date(2020, 7, 4), -2) == date(2020, 5, 1)
    assert get_month(date(2020, 7, 4), -3) == date(2020, 4, 1)
    assert get_month(date(2020, 7, 4), -4) == date(2020, 3, 1)
    assert get_month(date(2020, 7, 4), -5) == date(2020, 2, 1)
    assert get_month(date(2020, 7, 4), -6) == date(2020, 1, 1)
    assert get_month(date(2020, 7, 4), -7) == date(2019, 12, 1)
    assert get_month(date(2020, 7, 4), -8) == date(2019, 11, 1)
    assert get_month(date(2020, 7, 4), -9) == date(2019, 10, 1)
