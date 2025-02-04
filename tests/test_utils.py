import unittest
from decimal import Decimal
from unittest.mock import patch

from paradex_api import Api
from utils import get_random_position_size


class TestGetRandomPositionSize(unittest.TestCase):

    @patch("os.getenv")
    def test_get_random_position_size(self, mock_getenv):
        Api.max_buy_price = Decimal("99000")
        mock_getenv.side_effect = lambda key: {
            "SIZE_LIMIT": "0.004",
            "MIN_SIZE": "0.002",
            "SIZE_ROUND": "3",
            "PRICE_STEP": "0.1"
        }.get(key)

        balance = Decimal("10000")

        for _ in range(100):
            result = get_random_position_size(balance)
            self.assertGreaterEqual(result, Decimal("0.002"))  # MIN_SIZE
            self.assertLessEqual(result, Decimal("0.004"))  # SIZE_LIMIT
            print(f"Test #{_ + 1}: {result}")


if __name__ == "__main__":
    unittest.main()