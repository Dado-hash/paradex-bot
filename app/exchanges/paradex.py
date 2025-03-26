import asyncio
import logging
import os
from decimal import Decimal

from paradex_py import Paradex
from paradex_py.api.ws_client import ParadexWebsocketChannel
from paradex_py.common.order import OrderType, OrderSide, Order
from paradex_py.environment import PROD, TESTNET

from app.exchanges.base_exchange import BaseExchange
from app.helpers.utils import get_attribute
from app.models.data_order import DataOrder
from app.models.data_position import DataPosition
from app.models.position_side import PositionSide


class ParadexExchange(BaseExchange):
    _client: Paradex

    def __init__(self, L1_ADDRESS: str, L2_PRIVATE_KEY: str):
        API_ENV = PROD if os.getenv("IS_DEV").lower() == "false" else TESTNET
        self._client = Paradex(env=API_ENV, l1_address=L1_ADDRESS,
                               l2_private_key=L2_PRIVATE_KEY)

    async def setup(self):
        await self.__init_data_streams()
        if os.getenv("INITIAL_CLOSE_ALL_POSITIONS").lower() == "true":
            await self.critical_close_all()
        else:
            self.cancel_all_orders()
        await asyncio.sleep(10)

    async def __init_data_streams(self):
        await self._client.ws_client.connect()
        await self._client.ws_client.subscribe(ParadexWebsocketChannel.ORDER_BOOK,
                                               self.__price_websocket,
                                               params={"market": os.getenv("MARKET"), "refresh_rate": "50ms"})
        await self._client.ws_client.subscribe(ParadexWebsocketChannel.ORDERS,
                                               self.__orders_websocket,
                                               params={"market": os.getenv("MARKET")})
        await self._client.ws_client.subscribe(ParadexWebsocketChannel.POSITIONS,
                                               self.__positions_websocket)
        await self._client.ws_client.subscribe(ParadexWebsocketChannel.MARKETS_SUMMARY,
                                               self.__market_summary_websocket)
        await self._client.ws_client.subscribe(ParadexWebsocketChannel.ACCOUNT,
                                               self.__account_balance_websocket)
        asyncio.create_task(self.__reinit_data())

    async def __reinit_data(self):
        while True:
            try:
                self.open_orders = [self.__dict_to_order(order) for order in
                                    self._client.api_client.fetch_orders()['results'] if
                                    order['status'] == 'OPEN']
                self.open_positions = [self.__dict_to_position(position) for position in
                                       self._client.api_client.fetch_positions()['results'] if
                                       position['status'] == 'OPEN']
            except Exception as e:
                logging.exception(e)
            await asyncio.sleep(30)

    async def __price_websocket(self, _, message) -> None:
        arr = message['params']['data']['inserts']

        self.buy_orders_list = sorted(
            [(Decimal(x["price"]), Decimal(x["size"])) for x in arr if x["side"] == "BUY"],
            key=lambda order: order[0],
            reverse=True
        )

        self.sell_orders_list = sorted(
            [(Decimal(x["price"]), Decimal(x["size"])) for x in arr if x["side"] == "SELL"],
            key=lambda order: order[0],
        )

    async def __handle_websocket_data(self, data: dict, items_list: str) -> None:
        items: [dict] = self.__getattribute__(items_list)

        if data["status"] == "CLOSED":
            items = [x for x in items if x["id"] != data["id"]]
        elif data["status"] == "OPEN":
            if any(item["id"] == data["id"] for item in items):
                items = [data if x["id"] == data["id"] else x for x in items]
            else:
                items.append(data)
        if items_list == "open_orders":
            self.__setattr__(items_list, [self.__dict_to_order(x) for x in items])
        elif items_list == "open_positions":
            self.__setattr__(items_list, [self.__dict_to_position(x) for x in items])

    async def __orders_websocket(self, _, message) -> None:
        data = message["params"]["data"]
        await self.__handle_websocket_data(data, "open_orders")

    def __dict_to_order(self, data: dict):
        return DataOrder(id=data["id"], side=OrderSide(data["side"]), price=Decimal(data["price"]),
                         size=Decimal(data["size"]))

    async def __positions_websocket(self, _, message) -> None:
        data = message["params"]["data"]
        await self.__handle_websocket_data(data, "open_positions")

    def __dict_to_position(self, data: dict):
        return DataPosition(id=data["id"], market=data["market"], size=Decimal(data["size"]),
                            side=PositionSide(data["side"]),
                            average_entry_price=Decimal(data["average_entry_price"]),
                            created_at=get_attribute(data, "created_at"))

    async def __market_summary_websocket(self, _, message: dict) -> None:
        data = message["params"]["data"]
        if data['symbol'] == os.getenv("MARKET"):
            self.mark_price = round(Decimal(data['mark_price']), int(os.getenv("PRICE_ROUND")))

    async def __account_balance_websocket(self, _, message: dict) -> None:
        data = message["params"]["data"]
        if "account_value" in data:
            self.balance = Decimal(data["account_value"])

    def modify_limit_order(self, order_id: str, order_side: OrderSide, order_size: Decimal, price: Decimal,
                           is_reduce: bool = False):
        try:
            request_order = Order(
                order_id=order_id,
                market=os.getenv("MARKET"),
                order_type=OrderType.Limit,
                order_side=order_side,
                size=order_size,
                limit_price=price,
                instruction="POST_ONLY",
                reduce_only=is_reduce
            )
            self._client.api_client.modify_order(order_id, request_order)
        except Exception as e:
            logging.exception(e)

    def open_limit_order(self, order_side: OrderSide, order_size: Decimal, price: Decimal,
                         is_reduce: bool = False) -> str | None:
        try:
            request_order = Order(
                market=os.getenv("MARKET"),
                order_type=OrderType.Limit,
                order_side=order_side,
                size=order_size,
                limit_price=price,
                instruction="POST_ONLY",
                reduce_only=is_reduce
            )
            self._client.api_client.submit_order(request_order)
        except Exception as e:
            logging.exception(e)

    def open_market_order(self, order_side: OrderSide, order_size: Decimal, is_reduce: bool = False):
        try:
            request_order = Order(
                market=os.getenv("MARKET"),
                order_type=OrderType.Limit,
                order_side=order_side,
                size=order_size,
                limit_price=self.buy_orders_list[0][0] if order_side == OrderSide.Sell else self.sell_orders_list[0][0],
                reduce_only=is_reduce,
                instruction="IOC"
            )
            self._client.api_client.submit_order(request_order)
        except Exception as e:
            logging.exception(e)

    def cancel_all_orders(self):
        try:
            self._client.api_client.cancel_all_orders()
        except Exception as e:
            logging.exception(e)

    def close_all_positions(self):
        for position in self.open_positions:
            if abs(Decimal(position.size)) > 0:
                order = Order(
                    market=position.market,
                    order_type=OrderType.Market,
                    order_side=OrderSide.Sell if position.side.value == 'LONG' else OrderSide.Buy,
                    size=abs(Decimal(position.size)),
                    reduce_only=True
                )
                self._client.api_client.submit_order(order=order)

    async def critical_close_all(self):
        while True:
            try:
                self.cancel_all_orders()
                await asyncio.sleep(1)
                self.close_all_positions()
                logging.critical("All orders + positions closed")
                return
            except Exception as e:
                logging.exception(e)
                await asyncio.sleep(1)
