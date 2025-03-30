import os
from decimal import Decimal

import pytest

from app.bots.parallel_market_maker_bot import get_depth
from app.models.data_position import DataPosition


@pytest.fixture(scope="module", autouse=True)
def mock_env():
    os.environ["DEFAULT_DEPTH_ORDER_BOOK_ANALYSIS"] = "5"


def test_get_depth_both_positions_none():
    assert get_depth(None, None) == 5


def test_get_depth_different_sizes():
    main_pos = DataPosition(size=Decimal(10))
    other_pos = DataPosition(size=Decimal(5))
    assert get_depth(main_pos, other_pos) == 0


def test_get_depth_main_not_none_other_none():
    main_pos = DataPosition(size=Decimal(10))
    assert get_depth(main_pos, None) == 0


def test_get_depth_main_none_other_not_none():
    other_pos = DataPosition(size=Decimal(5))
    assert get_depth(None, other_pos) == 0
