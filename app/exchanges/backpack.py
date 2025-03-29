import asyncio
import logging
import os
from _decimal import Decimal

from backpack_exchange_sdk.authenticated import AuthenticationClient
from backpack_exchange_sdk.public import PublicClient
from backpack_exchange_sdk.websocket import WebSocketClient
from enums.RequestEnums import OrderType, OrderSide, TimeInForce

from app.exchanges.base_exchange import BaseExchange
from app.helpers.utils import get_time_now_milliseconds
from app.models.data_order import DataOrder
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum
from app.models.generic_position_side import GenericPositionSide


class BackpackExchange(BaseExchange):
    _client: AuthenticationClient
    _pub_clint: PublicClient
    _ws_client: WebSocketClient

    @property
    def balance(self):
        return Decimal(self._client.get_collateral()["netEquity"])

    def __init__(self, API_KEY: str, API_SECRET: str):
        self.exchange_type = ExchangeType.BACKPACK
        self._client = AuthenticationClient(API_KEY, API_SECRET)
        self._pub_clint = PublicClient()
        self.ws_client = WebSocketClient(API_KEY, API_SECRET)

    async def setup(self):
        await self.__init_data_streams()
        if os.getenv("INITIAL_CLOSE_ALL_POSITIONS").lower() == "true":
            await self.critical_close_all()
        else:
            self.cancel_all_orders()
        await asyncio.sleep(10)

    async def __init_data_streams(self):
        asyncio.create_task(self.__market_order_book_loop())
        market = os.getenv("BACKPACK_MARKET")

        self.ws_client.subscribe(streams=[f"account.orderUpdate.{market}"], callback=self.__orders_websocket,
                                 is_private=True)
        self.ws_client.subscribe(streams=[f"account.positionUpdate.{market}"], callback=self.__positions_websocket,
                                 is_private=True)
        self.ws_client.subscribe(streams=[f"markPrice.{market}"], callback=self.__market_summary_websocket)
        asyncio.create_task(self.__init_data_loop())

    def __init_data(self):
        try:
            self.open_positions = [self.__dict_to_position(
                {"i": x["positionId"], "s": x["symbol"], "q": x["netQuantity"], "B": x["entryPrice"],
                 "created_at": self.get_open_position_by_id(x["positionId"]).created_at if self.get_open_position_by_id(
                     x["positionId"]) else get_time_now_milliseconds()})
                for x in
                self._client.get_open_positions()]
        except Exception as e:
            logging.exception(e)

    async def __init_data_loop(self):
        while True:
            self.__init_data()
            await asyncio.sleep(30)

    def get_open_position_by_id(self, position_id: str) -> DataPosition | None:
        return next((item for item in self.open_positions if item.id == position_id), None)

    async def __market_order_book_loop(self):
        while True:
            try:
                data = self._pub_clint.get_depth(os.getenv("BACKPACK_MARKET"))
                bids = data["bids"]
                asks = data["asks"]

                self.buy_orders_list = sorted(
                    [(Decimal(x[0]), Decimal(x[1])) for x in bids],
                    key=lambda order: order[0],
                    reverse=True
                )

                self.sell_orders_list = sorted(
                    [(Decimal(x[0]), Decimal(x[1])) for x in asks],
                    key=lambda order: order[0],
                )
            except Exception as e:
                logging.exception(e)
            await asyncio.sleep(float(os.getenv("PING_SECONDS")))

    def __orders_websocket(self, data: dict) -> None:
        try:
            if (not "e" in data) or data['o'] == "MARKET":
                return
            if data["e"] == "orderModified":
                if Decimal(data["l"]) > 0:
                    if any(x.id == data["i"] for x in self.open_orders):
                        self.open_orders = [
                            self.__dict_to_order(data) if x.id == data["i"] else x for x in self.open_orders]
                    else:
                        self.open_orders.append(self.__dict_to_order(data))
                else:
                    self.open_orders = [x for x in self.open_orders if x.id != data["i"]]
            elif data["e"] == "orderFill":
                if any(x.id == data["i"] and x.size > Decimal(data["l"]) for x in self.open_orders):
                    self.open_orders = [
                        self.__dict_to_order({**data, 'q': x.size - Decimal(x['l']), 'p': x.price}) if x.id == data[
                            "i"] else x for x in
                        self.open_orders]
                else:
                    self.open_orders = [x for x in self.open_orders if x.id != data["i"]]
            elif data["e"] == "orderAccepted":
                self.open_orders.append(self.__dict_to_order(data))
            else:  # Closed
                self.open_orders = [x for x in self.open_orders if x.id != data["i"]]
        except Exception as e:
            logging.exception(e)

    def __dict_to_order(self, data: dict):
        if isinstance(data, DataOrder):
            return data
        return DataOrder(id=data["i"], side=GenericOrderSide(data["S"], ExchangeType.BACKPACK),
                         price=Decimal(data["p"]),
                         size=Decimal(data["q"]))

    def __positions_websocket(self, data: dict) -> None:
        try:
            if not "e" in data:
                return
            if data["e"] == "positionAdjusted":
                if any(x.id == data["i"] for x in self.open_positions):
                    self.open_positions = [
                        self.__dict_to_position(
                            {**data, "created_at": x.created_at}) if x.id == data["i"] else x for x in
                        self.open_positions]
                else:
                    self.open_positions.append(
                        self.__dict_to_position({**data, "created_at": get_time_now_milliseconds()}))
            elif data["e"] == "positionOpened":
                self.open_positions.append(self.__dict_to_position(data))
            else:  # Closed
                self.open_positions = [x for x in self.open_positions if x.id != data["i"]]
        except Exception as e:
            logging.exception(e)

    def __dict_to_position(self, data: dict | DataPosition):
        if isinstance(data, DataPosition):
            return data

        return DataPosition(id=data["i"], market=data["s"], size=Decimal(data["q"]),
                            side=GenericPositionSide(Decimal(data["q"]) > 0, ExchangeType.BACKPACK),
                            average_entry_price=Decimal(data["B"]),
                            created_at=str(float(data["E"]) / 1000) if "e" in data and data[
                                "e"] == "positionOpened" else data["created_at"])

    def __market_summary_websocket(self, data: dict) -> None:
        self.mark_price = round(Decimal(data["p"]), int(os.getenv("BACKPACK_PRICE_ROUND")))

    def modify_limit_order(self, order_id: str, order_side: GenericOrderSide, order_size: Decimal, price: Decimal,
                           is_reduce: bool = False) -> dict | None:
        try:
            self._client.cancel_open_order(symbol=os.getenv("BACKPACK_MARKET"), orderId=order_id)

            self.open_limit_order(order_side, order_size, price, is_reduce)
        except Exception as e:
            logging.exception(e)
            self.cancel_all_orders()

    def open_limit_order(self, order_side: GenericOrderSide, order_size: Decimal, price: Decimal,
                         is_reduce: bool = False) -> dict | None:
        try:
            self._client.execute_order(
                symbol=os.getenv("BACKPACK_MARKET"),
                orderType=OrderType.LIMIT,
                side=OrderSide(order_side.value),
                quantity=str(order_size),
                price=str(price),
                postOnly=True,
                reduceOnly=is_reduce,
            )
        except Exception as e:
            logging.exception(e)
            self.cancel_all_orders()

    def open_market_order(self, order_side: GenericOrderSide, order_size: Decimal, is_reduce: bool = False):
        try:
            self._client.execute_order(
                symbol=os.getenv("BACKPACK_MARKET"),
                orderType=OrderType.LIMIT,
                side=OrderSide(order_side.value),
                quantity=str(order_size),
                price=self.buy_orders_list[0][0] if order_side == OrderSideEnum.SELL else self.sell_orders_list[0][0],
                reduceOnly=is_reduce,
                timeInForce=TimeInForce.IOC
            )
        except Exception as e:
            logging.exception(e)
            self.cancel_all_orders()

    def cancel_all_orders(self) -> None:
        try:
            self._client.cancel_open_orders(os.getenv("BACKPACK_MARKET"))
            self.open_orders = []
        except Exception as e:
            logging.exception(e)

    def close_all_positions(self) -> None:
        for position in self.open_positions:
            if abs(Decimal(position.size)) > 0:
                self._client.execute_order(
                    symbol=os.getenv("BACKPACK_MARKET"),
                    orderType=OrderType.MARKET,
                    side=OrderSide(position.side.opposite_order_side().value),
                    quantity=str(abs(Decimal(position.size))),
                    reduceOnly=True
                )

    async def critical_close_all(self) -> None:
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
