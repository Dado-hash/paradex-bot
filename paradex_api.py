import asyncio
import logging
import os
from decimal import Decimal

from paradex_py import Paradex
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.common.order import OrderType, Order, OrderSide
from paradex_py.environment import PROD, TESTNET


class Api:
    paradex: Paradex | None = None

    max_buy_price: Decimal | None = None
    pre_max_buy_price: Decimal | None = None

    min_sell_price: Decimal | None = None
    pre_min_sell_price: Decimal | None = None

    last_buy_size: Decimal | None = None
    pre_last_buy_size: Decimal | None = None
    last_sell_size: Decimal | None = None
    pre_last_sell_size: Decimal | None = None

    @staticmethod
    async def init_paradex() -> None:
        API_ENV = PROD if os.getenv("IS_DEV").lower() == "false" else TESTNET
        Api.paradex = Paradex(env=API_ENV, l1_address=os.getenv("L1_ADDRESS"),
                              l2_private_key=os.getenv("L2_PRIVATE_KEY"))

    @staticmethod
    async def init_price_websocket() -> None:
        await Api.paradex.ws_client.connect()
        await Api.paradex.ws_client.subscribe(ParadexWebsocketChannel.ORDER_BOOK, callback=Api.price_websocket,
                                              params={"market": os.getenv("COIN")})

    @staticmethod
    async def price_websocket(ws_channel, message) -> None:
        arr = message['params']['data']['inserts']

        buy_prices_list = sorted(
            [Decimal(str(x["price"])) for x in arr if x["side"] == "BUY"],
            reverse=True
        )

        buy_size_list = [
            Decimal(x["size"]) for x in sorted(arr, key=lambda x: Decimal(str(x["price"])), reverse=True) if
            x["side"] == "BUY"
        ]

        if buy_prices_list:
            Api.max_buy_price = buy_prices_list[0]
            Api.last_buy_size = buy_size_list[0]

            Api.pre_max_buy_price = buy_prices_list[1] if len(buy_prices_list) > 1 else Api.max_buy_price
            Api.pre_last_buy_size = buy_size_list[1] if len(buy_size_list) > 1 else Api.last_buy_size

        sell_prices_list = sorted(
            [Decimal(str(x["price"])) for x in arr if x["side"] == "SELL"]
        )

        sell_size_list = [
            Decimal(x["size"]) for x in sorted(arr, key=lambda x: Decimal(str(x["price"]))) if x["side"] == "SELL"
        ]

        if sell_prices_list:
            Api.min_sell_price = sell_prices_list[0]
            Api.last_sell_size = sell_size_list[0]

            Api.pre_min_sell_price = sell_prices_list[1] if len(sell_prices_list) > 1 else Api.min_sell_price
            Api.pre_last_sell_size = sell_size_list[1] if len(sell_size_list) > 1 else Api.last_sell_size

        # logging.info(
        #     str(Api.pre_max_buy_price) + "-" + str(Api.max_buy_price)
        #     + " :BUY(" + str(Api.last_buy_size) + ")|SELL(" + str(Api.last_sell_size) + "): " +
        #     str(Api.min_sell_price) + "-" + str(Api.pre_min_sell_price))

    @staticmethod
    def get_best_price(side: OrderSide, order_size: Decimal, order_price: Decimal = Decimal(0)) -> Decimal:
        step = Decimal(os.getenv("PRICE_STEP"))
        max_gap = step * 5

        if side == OrderSide.Buy:
            max_gap = max_gap - step if order_price == Api.pre_max_buy_price else max_gap
            # If there is a large gap between the first and second buy order
            if Api.pre_max_buy_price + max_gap < Api.max_buy_price:
                # If the second order is fully mine, do not move it
                if Api.pre_max_buy_price == order_price and Api.pre_last_buy_size == order_size:
                    return Api.pre_max_buy_price
                return Api.pre_max_buy_price + step

            # If my order is already first and it's the only one, keep it
            if order_price == Api.max_buy_price and order_size == Api.last_buy_size:
                return Api.max_buy_price

            # If I don’t have an order, try to place it first
            next_max_price = Api.max_buy_price + step
            if next_max_price >= Api.min_sell_price:
                return Api.max_buy_price  # Do not cross the spread

            return next_max_price

        else:  # Sell side
            max_gap = max_gap + step if order_price == Api.pre_min_sell_price else max_gap
            # If there is a large gap between the first and second sell order
            if Api.pre_min_sell_price - max_gap > Api.min_sell_price:
                # If the second order is fully mine, do not move it
                if Api.pre_min_sell_price == order_price and Api.pre_last_sell_size == order_size:
                    return Api.pre_min_sell_price
                return Api.pre_min_sell_price - step

            # If my order is already first and it's the only one, keep it
            if order_price == Api.min_sell_price and order_size == Api.last_sell_size:
                return Api.min_sell_price

            # If I don’t have an order, try to place it first
            next_min_price = Api.min_sell_price - step
            if next_min_price <= Api.max_buy_price:
                return Api.min_sell_price  # Do not cross the spread

            return next_min_price

    @staticmethod
    def get_USDC_balance() -> Decimal:
        results = Api.paradex.api_client.fetch_balances()['results']
        return Decimal(str(next((item['size'] for item in results if item['token'] == 'USDC'))))

    @staticmethod
    def get_position_size() -> Decimal:
        positions = Api.paradex.api_client.fetch_positions()["results"]
        open_positions = list(filter(lambda x: x["status"] == "OPEN", positions))

        if not open_positions:
            return Decimal(0)

        return abs(Decimal(open_positions[0]["size"]))

    @staticmethod
    async def is_any_positions_open_critical(retry=0) -> bool:
        size = Api.get_position_size()
        if size == Decimal(0):
            if retry > 3:
                return False
            await asyncio.sleep(0.5)
            return await Api.is_any_positions_open_critical(retry + 1)
        return True

    @staticmethod
    async def fetch_order_by_client_id(open_order_client_id, retry=0) -> dict | bool:
        try:
            order = Api.paradex.api_client.fetch_order_by_client_id(open_order_client_id)
            return order
        except Exception as e:
            logging.critical(e)
            if retry <= 3:
                await asyncio.sleep(0.5)
                return await Api.fetch_order_by_client_id(open_order_client_id, retry + 1)
            is_position_opened = await Api.is_any_positions_open_critical()
            return is_position_opened

    @staticmethod
    async def cancel_order_by_client_id(open_order_client_id, retry=0) -> None | bool:
        try:
            Api.paradex.api_client.cancel_order_by_client_id(open_order_client_id)
        except Exception as e:
            logging.critical(e)
            if retry <= 3:
                await asyncio.sleep(0.5)
                return await Api.cancel_order_by_client_id(open_order_client_id, retry + 1)
            is_position_opened = await Api.is_any_positions_open_critical()
            return is_position_opened

    @staticmethod
    async def close_all_positions() -> None:
        open_positions = filter(lambda x: x["status"] == "OPEN", Api.paradex.api_client.fetch_positions()["results"])

        for position in open_positions:
            order = Order(
                market=position['market'],
                order_type=OrderType.Market,
                order_side=OrderSide.Sell if position['side'] == 'LONG' else OrderSide.Buy,
                size=abs(Decimal(position['size'])),
                reduce_only=True
            )
            Api.paradex.api_client.submit_order(order=order)

            await asyncio.sleep(0.1)

    @staticmethod
    async def critical_close() -> None:
        while True:
            try:
                Api.paradex.api_client.cancel_all_orders()
                await Api.close_all_positions()
                logging.critical("All orders + positions closed")
                return
            except Exception as e:
                logging.critical(e)
                await asyncio.sleep(1)
