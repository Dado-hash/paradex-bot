import asyncio
import logging
import os
import json
import hmac
import hashlib
import time
import uuid
from decimal import Decimal
from typing import Optional, Dict, Any

import aiohttp
import websockets
from eth_account import Account
from eth_account.messages import encode_defunct

from app.exchanges.base_exchange import BaseExchange
from app.helpers.utils import get_attribute, get_time_now_milliseconds
from app.models.data_order import DataOrder
from app.models.data_position import DataPosition
from app.models.exchange_type import ExchangeType
from app.models.generic_order_side import GenericOrderSide, OrderSideEnum
from app.models.generic_position_side import GenericPositionSide


class HibachiExchange(BaseExchange):
    def __init__(self, api_key: str, private_key: str):
        self.exchange_type = ExchangeType.HIBACHI
        self.api_key = api_key
        self.private_key = private_key

        # Debug logging
        logging.info(f"Hibachi initialized with API key: {'***' if api_key else 'None'}")
        logging.info(f"Hibachi initialized with Private key: {'***' if private_key else 'None'}")
        self.market_base_url = "https://data-api.hibachi.xyz"  # For market data
        self.trade_base_url = "https://api.hibachi.xyz"      # For trading operations
        self.ws_url = "wss://data-api.hibachi.xyz"
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws_connections = {}
        self._balance = Decimal('0')

    async def setup(self):
        self._session = aiohttp.ClientSession()
        await self.__init_data_streams()
        if os.getenv("INITIAL_CLOSE_ALL_POSITIONS", "false").lower() == "true":
            await self.critical_close_all()
        else:
            self.cancel_all_orders()
        await asyncio.sleep(10)

    def _generate_signature(self, method: str, path: str, params: str = "", body: str = "") -> str:
        """Generate HMAC signature for authenticated requests"""
        if not self.private_key:
            logging.error("Private key is None, cannot generate signature")
            return str(int(time.time() * 1000)), ""

        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method.upper()}{path}{params}{body}"

        # Handle both hex string (with 0x) and plain hex
        private_key_bytes = self.private_key
        if private_key_bytes.startswith('0x'):
            private_key_bytes = private_key_bytes[2:]

        signature = hmac.new(
            bytes.fromhex(private_key_bytes),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return timestamp, signature

    def _sign_order(self, order_data: Dict[str, Any]) -> str:
        """Generate ECDSA signature for order according to Hibachi spec"""
        try:
            import struct
            from Crypto.Hash import keccak

            # Convert values to the binary format expected by Hibachi
            nonce = int(order_data.get('nonce', 0))
            contract_id = 2  # Fixed contractId for orders

            # Convert quantity and price from string to proper format
            quantity_str = str(order_data.get('quantity', '0'))
            price_str = str(order_data.get('price', '0'))

            # Parse quantity and price as floats then convert to fixed-point
            quantity_float = float(quantity_str)
            price_float = float(price_str) if price_str else 0

            # For HYPE, we need to get decimals from /market/exchange-info
            # From docs example: underlyingDecimals=10, settlementDecimals=6
            # For now using standard values, should get from API in production
            underlying_decimals = 10  # BTC/ETH typically 10
            settlement_decimals = 6   # USDT typically 6
            price_multiplier = 2**32  # Fixed multiplier

            # Convert quantity: quantity * 10^underlyingDecimals
            quantity_fixed = int(quantity_float * (10 ** underlying_decimals))

            # Convert price: price * 2^32 * 10^(settlementDecimals - underlyingDecimals)
            if price_float > 0:
                price_exponent = settlement_decimals - underlying_decimals  # 6 - 10 = -4
                price_fixed = int(price_float * price_multiplier * (10 ** price_exponent))
            else:
                price_fixed = 0

            # Side: ASK=0, BID=1 (as per documentation)
            side_str = order_data.get('side', 'ASK')
            side_value = 1 if side_str == 'BID' else 0

            # Max fees: convert from percentage to basis points * 10^8
            # 0.00045 = 45 basis points = 45 * 10^6 (per docs example)
            max_fees_percent = float(order_data.get('maxFeesPercent', '0.00045'))
            fees_fixed = int(max_fees_percent * 10**8)

            # Create binary buffer according to Hibachi spec (page 7)
            # Order: nonce(8) + contractId(4) + quantity(8) + side(4) + price(8) + fees(8)
            buffer = b''
            buffer += struct.pack('>Q', nonce)           # 8 bytes - nonce
            buffer += struct.pack('>I', contract_id)     # 4 bytes - contractId
            buffer += struct.pack('>Q', quantity_fixed)  # 8 bytes - quantity
            buffer += struct.pack('>I', side_value)      # 4 bytes - side
            buffer += struct.pack('>Q', price_fixed)     # 8 bytes - price
            buffer += struct.pack('>Q', fees_fixed)      # 8 bytes - maxFeesPercent

            # Create keccak256 hash of the buffer (NOT SHA256)
            hash_obj = keccak.new(digest_bits=256)
            hash_obj.update(buffer)
            msg_hash = hash_obj.digest()

            # Sign with ECDSA
            account = Account.from_key(self.private_key)
            signed_message = account.signHash(msg_hash)

            # Try different signature formats to see what Hibachi expects
            # Option 1: Full signature with recovery ID (65 bytes = 130 hex chars)
            full_signature = signed_message.signature.hex()
            
            # Option 2: Just r,s without recovery ID (64 bytes = 128 hex chars)  
            r = signed_message.r
            s = signed_message.s
            rs_signature = f"{r:064x}{s:064x}"
            
            # Option 3: r,s,v format where v is recovery + 27
            v = signed_message.v
            rsv_signature = f"{r:064x}{s:064x}{v:02x}"
            
            # For now, let's try the full signature first (what we had before)
            signature = full_signature
            
            # Ensure 0x prefix is present - but avoid double prefix!
            if not signature.startswith('0x'):
                signature = '0x' + signature
                
            # Log all formats for debugging
            logging.info(f"Signature debugging:")
            logging.info(f"  Full signature (65 bytes): {full_signature} (len: {len(full_signature)})")
            logging.info(f"  R,S signature (64 bytes): {rs_signature} (len: {len(rs_signature)})")
            logging.info(f"  R,S,V signature: {rsv_signature} (len: {len(rsv_signature)})")
            logging.info(f"  Final signature: {signature} (len: {len(signature)})")
            logging.info(f"  Recovery ID (v): {v}")
            
            # If this fails, we can try the r,s format by uncommenting below:
            # signature = '0x' + rs_signature

            logging.info(f"Hibachi order signing (corrected):")
            logging.info(f"  Nonce: {nonce} -> {hex(nonce)}")
            logging.info(f"  ContractId: {contract_id} -> {hex(contract_id)}")
            logging.info(f"  Quantity: {quantity_float} -> {quantity_fixed} -> {hex(quantity_fixed)}")
            logging.info(f"  Side: {side_str} -> {side_value} -> {hex(side_value)}")
            logging.info(f"  Price: {price_float} -> {price_fixed} -> {hex(price_fixed)}")
            logging.info(f"  Fees: {max_fees_percent} -> {fees_fixed} -> {hex(fees_fixed)}")
            logging.info(f"  Buffer: {buffer.hex()}")
            logging.info(f"  Keccak256: {msg_hash.hex()}")
            logging.info(f"  Signature: {signature} (length: {len(signature)} chars)")

            return signature

        except Exception as e:
            logging.error(f"Error signing Hibachi order: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return "0000000000000000000000000000000000000000000000000000000000000000"

    async def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, data: Optional[Dict] = None, auth: bool = True, use_trade_api: bool = False) -> Optional[Dict]:
        """Make authenticated HTTP request"""
        if not self._session:
            return None

        # Choose the correct base URL
        base_url = self.trade_base_url if use_trade_api else self.market_base_url
        url = f"{base_url}{endpoint}"
        headers = {"Content-Type": "application/json"}

        # Debug logging
        logging.info(f"Hibachi API Request: {method} {url} params={params} auth={auth}")

        if auth:
            # Hibachi uses simple Authorization header, not HMAC signature
            headers.update({
                "Authorization": self.api_key
            })

        try:
            async with self._session.request(
                method=method,
                url=url,
                params=params,
                json=data,
                headers=headers
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    response_text = await response.text()
                    logging.error(f"HTTP {response.status}: {response_text} - URL: {url}")
                    return None
        except Exception as e:
            logging.error(f"Request error: {e}")
            return None

    async def __init_data_streams(self):
        """Initialize REST-only data streams for Hibachi"""
        try:
            logging.info("Initializing Hibachi REST-only data streams...")

            # Skip WebSocket connections, use REST API only
            # Data collection loop (primary method)
            asyncio.create_task(self.__init_data_loop())

            # Also get initial market data
            asyncio.create_task(self._get_initial_market_data())

            logging.info("Hibachi REST data streams initialized")

        except Exception as e:
            logging.error(f"Error initializing data streams: {e}")

    async def _get_initial_market_data(self):
        """Get initial market data via REST API"""
        try:
            market = os.getenv("HIBACHI_MARKET", "BTCUSDT")

            # Get order book data with correct endpoint and params
            orderbook_data = await self._make_request("GET", f"/market/data/prices", params={"symbol": market}, auth=False)
            if orderbook_data:
                # Update basic order book from ticker
                if 'bidPrice' in orderbook_data and 'askPrice' in orderbook_data:
                    bid_price = Decimal(str(orderbook_data['bidPrice']))
                    ask_price = Decimal(str(orderbook_data['askPrice']))
                    bid_qty = Decimal(str(orderbook_data.get('bidQty', '1')))
                    ask_qty = Decimal(str(orderbook_data.get('askQty', '1')))

                    self.buy_orders_list = [(bid_price, bid_qty)]
                    self.sell_orders_list = [(ask_price, ask_qty)]
                    self.mark_price = (bid_price + ask_price) / 2

                    logging.info(f"Initial market data loaded: bid={bid_price}, ask={ask_price}")

            # Get more detailed order book if available
            depth_data = await self._make_request("GET", f"/market/data/orderbook",
                                                params={"symbol": market, "limit": 10}, auth=False)
            if depth_data and 'bids' in depth_data and 'asks' in depth_data:
                self.buy_orders_list = [
                    (Decimal(str(bid[0])), Decimal(str(bid[1])))
                    for bid in depth_data['bids'][:10]
                ]
                self.sell_orders_list = [
                    (Decimal(str(ask[0])), Decimal(str(ask[1])))
                    for ask in depth_data['asks'][:10]
                ]

                if self.buy_orders_list and self.sell_orders_list:
                    best_bid = self.buy_orders_list[0][0]
                    best_ask = self.sell_orders_list[0][0]
                    self.mark_price = (best_bid + best_ask) / 2

                logging.info(f"Detailed order book loaded: {len(self.buy_orders_list)} bids, {len(self.sell_orders_list)} asks")

        except Exception as e:
            logging.error(f"Error getting initial market data: {e}")

    async def _connect_market_ws(self):
        """Connect to market data WebSocket"""
        try:
            market_symbol = os.getenv("HIBACHI_MARKET", "BTCUSDT")
            # Try multiple possible WebSocket URL patterns for data-api
            possible_urls = [
                f"{self.ws_url}/ws/market",  # Standard pattern
                f"{self.ws_url}/ws/stream",
                f"{self.ws_url}/stream",
                f"{self.ws_url}/ws",
                "wss://data-api.hibachi.xyz/ws/market",
                "wss://ws.hibachi.xyz/market"
            ]

            for ws_url in possible_urls:
                try:
                    logging.info(f"Trying WebSocket URL: {ws_url}")
                    async with websockets.connect(ws_url, timeout=10) as websocket:
                        self._ws_connections['market'] = websocket
                        logging.info(f"✅ Market WebSocket connected: {ws_url}")

                        # Subscribe to order book
                        subscribe_msg = {
                            "method": "SUBSCRIBE",
                            "params": [f"{market_symbol.lower()}@depth"],
                            "id": 1
                        }
                        await websocket.send(json.dumps(subscribe_msg))

                        async for message in websocket:
                            try:
                                data = json.loads(message)
                                await self._handle_market_data(data)
                            except Exception as e:
                                logging.error(f"Error processing market data: {e}")
                        return  # Success, exit the loop

                except Exception as url_error:
                    logging.warning(f"❌ Failed to connect to {ws_url}: {url_error}")
                    continue  # Try next URL

            # If all URLs failed
            logging.warning("⚠️ All WebSocket URLs failed, using REST API only for market data")

        except Exception as e:
            logging.error(f"Market WebSocket connection error: {e}")

        # Reconnect after delay
        await asyncio.sleep(5)
        # Don't reconnect automatically for now to avoid spam

    async def _connect_account_ws(self):
        """Connect to account data WebSocket"""
        try:
            # Try multiple possible WebSocket URL patterns for account data
            possible_urls = [
                f"{self.ws_url}/ws/account",  # Standard private endpoint
                f"{self.ws_url}/ws/user",
                f"{self.ws_url}/account",
                "wss://ws.hibachi.xyz/account"
            ]

            headers = {"X-API-KEY": self.api_key}

            for ws_url in possible_urls:
                try:
                    logging.info(f"Trying Account WebSocket URL: {ws_url}")
                    async with websockets.connect(ws_url, extra_headers=headers, timeout=10) as websocket:
                        self._ws_connections['account'] = websocket
                        logging.info(f"✅ Account WebSocket connected: {ws_url}")

                        # Subscribe to account updates
                        subscribe_msg = {
                            "method": "SUBSCRIBE",
                            "params": ["account.balance", "account.orders", "account.positions"],
                            "id": 2
                        }
                        await websocket.send(json.dumps(subscribe_msg))

                        async for message in websocket:
                            try:
                                data = json.loads(message)
                                await self._handle_account_data(data)
                            except Exception as e:
                                logging.error(f"Error processing account data: {e}")
                        return  # Success, exit the loop

                except Exception as url_error:
                    logging.warning(f"❌ Failed to connect to account WS {ws_url}: {url_error}")
                    continue  # Try next URL

            # If all URLs failed
            logging.warning("⚠️ All Account WebSocket URLs failed, using REST API only")

        except Exception as e:
            logging.error(f"Account WebSocket connection error: {e}")

        # Don't reconnect automatically for now

    async def _handle_market_data(self, data: Dict[Any, Any]):
        """Handle market data from WebSocket"""
        if 'bids' in data and 'asks' in data:
            # Update order book
            self.buy_orders_list = [
                (Decimal(str(bid[0])), Decimal(str(bid[1])))
                for bid in data['bids'][:10]
            ]
            self.sell_orders_list = [
                (Decimal(str(ask[0])), Decimal(str(ask[1])))
                for ask in data['asks'][:10]
            ]

            # Update mark price
            if self.buy_orders_list and self.sell_orders_list:
                best_bid = self.buy_orders_list[0][0]
                best_ask = self.sell_orders_list[0][0]
                self.mark_price = (best_bid + best_ask) / 2

    async def _handle_account_data(self, data: Dict[Any, Any]):
        """Handle account data from WebSocket"""
        if data.get('e') == 'balanceUpdate':
            self._balance = Decimal(str(data.get('balance', 0)))
        elif data.get('e') == 'orderUpdate':
            await self._update_orders_from_ws(data)
        elif data.get('e') == 'positionUpdate':
            await self._update_positions_from_ws(data)

    async def __init_data_loop(self):
        """Periodic data collection loop via REST API"""
        while True:
            try:
                # Use longer interval for REST API to avoid rate limiting
                await asyncio.sleep(max(float(os.getenv("PING_SECONDS", "0.3")), 2.0))

                market = os.getenv("HIBACHI_MARKET", "BTCUSDT")

                # Get market data (order book)
                depth_data = await self._make_request("GET", "/market/data/orderbook",
                                                    params={"symbol": market, "limit": 10}, auth=False)
                if depth_data and 'bids' in depth_data and 'asks' in depth_data:
                    self.buy_orders_list = [
                        (Decimal(str(bid[0])), Decimal(str(bid[1])))
                        for bid in depth_data['bids'][:10]
                    ]
                    self.sell_orders_list = [
                        (Decimal(str(ask[0])), Decimal(str(ask[1])))
                        for ask in depth_data['asks'][:10]
                    ]

                    if self.buy_orders_list and self.sell_orders_list:
                        best_bid = self.buy_orders_list[0][0]
                        best_ask = self.sell_orders_list[0][0]
                        self.mark_price = (best_bid + best_ask) / 2

                # Get account info (includes balance and positions)
                account_id = os.getenv("HIBACHI_ACCOUNT_ID")
                if account_id:
                    account_data = await self._make_request("GET", "/trade/account/info", params={"accountId": account_id}, use_trade_api=True)
                else:
                    account_data = await self._make_request("GET", "/trade/account/info", use_trade_api=True)

                if account_data:
                    # Update balance
                    if 'balance' in account_data:
                        self._balance = Decimal(str(account_data['balance']))

                    # Update positions from account info
                    if 'positions' in account_data:
                        self._update_positions(account_data['positions'])

                # Get open orders
                if account_id:
                    orders_data = await self._make_request("GET", "/trade/orders", params={"accountId": account_id}, use_trade_api=True)
                else:
                    orders_data = await self._make_request("GET", "/trade/orders", use_trade_api=True)
                if orders_data:
                    self._update_orders(orders_data)

            except Exception as e:
                logging.error(f"Error in data loop: {e}")
                await asyncio.sleep(1)

    def _update_orders(self, orders_data):
        """Update open orders from REST API response"""
        self.open_orders = []
        if not isinstance(orders_data, list):
            orders_data = orders_data.get('orders', [])

        for order in orders_data:
            try:
                data_order = DataOrder(
                    id=str(order.get('orderId')),
                    size=Decimal(str(order.get('quantity', 0))),
                    price=Decimal(str(order.get('price', 0))),
                    side=GenericOrderSide(OrderSideEnum.BUY if order.get('side', '').upper() == 'BUY' else OrderSideEnum.SELL),
                    created_at=get_time_now_milliseconds()
                )
                self.open_orders.append(data_order)
            except Exception as e:
                logging.error(f"Error parsing order: {e}")

    def _update_positions(self, positions_data):
        """Update positions from Hibachi account/info response"""
        self.open_positions = []
        if not isinstance(positions_data, list):
            positions_data = positions_data.get('positions', [])

        for position in positions_data:
            try:
                # Hibachi position structure
                quantity = Decimal(str(position.get('quantity', 0)))
                direction = position.get('direction', 'Long')  # "Long" or "Short"

                if quantity != 0:
                    # Calculate entry price from entryNotional and quantity
                    entry_notional = Decimal(str(position.get('entryNotional', 0)))
                    entry_price = entry_notional / quantity if quantity != 0 else Decimal('0')

                    # For Hibachi, direction determines side, quantity is always positive
                    side = GenericPositionSide.LONG if direction == 'Long' else GenericPositionSide.SHORT

                    # For short positions, represent size as negative for consistency
                    size = quantity if direction == 'Long' else -quantity

                    data_position = DataPosition(
                        size=size,
                        side=side,
                        entry_price=entry_price,
                        unrealized_pnl=Decimal(str(position.get('unrealizedTradingPnl', 0))),
                        created_at=get_time_now_milliseconds()
                    )
                    self.open_positions.append(data_position)
            except Exception as e:
                logging.error(f"Error parsing Hibachi position: {e}")

    async def _update_orders_from_ws(self, data):
        """Update orders from WebSocket data"""
        # Implementation for WebSocket order updates
        pass

    async def _update_positions_from_ws(self, data):
        """Update positions from WebSocket data"""
        # Implementation for WebSocket position updates
        pass

    def open_limit_order(self, order_side: GenericOrderSide, order_size: Decimal, price: Decimal, is_reduce: bool = False) -> dict | None:
        """Place a limit order"""
        try:
            market = os.getenv("HIBACHI_MARKET", "BTCUSDT")
            side = order_side.value  # Now correctly returns "BID" or "ASK"
            account_id = os.getenv("HIBACHI_ACCOUNT_ID")

            # Hibachi requires specific order structure
            nonce = int(time.time() * 1000000)  # microseconds timestamp
            client_id = f"bot-{uuid.uuid4().hex[:8]}"

            order_data = {
                "accountId": int(account_id) if account_id else 128,
                "symbol": market,
                "nonce": nonce,
                "orderType": "LIMIT",
                "side": side,  # "BID" or "ASK"
                "quantity": str(order_size),
                "price": str(price),
                "maxFeesPercent": "0.00045",
                "clientId": client_id
            }

            # Generate signature
            signature = self._sign_order(order_data)
            order_data["signature"] = signature

            # Debug log the complete order payload
            logging.info(f"Complete Hibachi order payload: {json.dumps(order_data, indent=2)}")

            # Add optional fields if needed
            if is_reduce:
                # Hibachi might have different field name for reduce only
                pass  # Check docs for correct field name

            # Convert to sync call for now - in production, make this async
            loop = asyncio.get_event_loop()
            result = loop.create_task(self._make_request("POST", "/trade/order", data=order_data, use_trade_api=True))

            logging.info(f"Placed limit order: {result}")
            return result

        except Exception as e:
            logging.error(f"Error placing limit order: {e}")
            return None

    def open_market_order(self, order_side: GenericOrderSide, order_size: Decimal, is_reduce: bool = False):
        """Place a market order"""
        try:
            market = os.getenv("HIBACHI_MARKET", "BTCUSDT")
            side = order_side.value  # Now correctly returns "BID" or "ASK"
            account_id = os.getenv("HIBACHI_ACCOUNT_ID")

            # Hibachi requires specific order structure
            nonce = int(time.time() * 1000000)  # microseconds timestamp
            client_id = f"bot-{uuid.uuid4().hex[:8]}"

            order_data = {
                "accountId": int(account_id) if account_id else 128,
                "symbol": market,
                "nonce": nonce,
                "orderType": "MARKET",
                "side": side,  # "BID" or "ASK"
                "quantity": str(order_size),
                "maxFeesPercent": "0.00045",
                "clientId": client_id
            }

            # Generate signature
            signature = self._sign_order(order_data)
            order_data["signature"] = signature

            # Debug log the complete order payload
            logging.info(f"Complete Hibachi order payload: {json.dumps(order_data, indent=2)}")

            # Add optional fields if needed
            if is_reduce:
                # Hibachi might have different field name for reduce only
                pass  # Check docs for correct field name

            loop = asyncio.get_event_loop()
            result = loop.create_task(self._make_request("POST", "/trade/order", data=order_data, use_trade_api=True))

            logging.info(f"Placed market order: {result}")
            return result

        except Exception as e:
            logging.error(f"Error placing market order: {e}")
            return None

    def modify_limit_order(self, order_id: str, order_side: GenericOrderSide, order_size: Decimal, price: Decimal, is_reduce: bool = False) -> dict | None:
        """Modify an existing order"""
        try:
            # Cancel existing order first
            self.cancel_order(order_id)
            # Place new order
            return self.open_limit_order(order_side, order_size, price, is_reduce)
        except Exception as e:
            logging.error(f"Error modifying order: {e}")
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a specific order"""
        try:
            market = os.getenv("HIBACHI_MARKET", "BTCUSDT")
            account_id = os.getenv("HIBACHI_ACCOUNT_ID")
            params = {"symbol": market, "orderId": order_id}
            if account_id:
                params["accountId"] = account_id

            loop = asyncio.get_event_loop()
            result = loop.create_task(self._make_request("DELETE", "/trade/order", params=params, use_trade_api=True))

            logging.info(f"Cancelled order {order_id}")
            return True
        except Exception as e:
            logging.error(f"Error cancelling order {order_id}: {e}")
            return False

    def cancel_all_orders(self) -> None:
        """Cancel all open orders"""
        try:
            for order in self.open_orders:
                self.cancel_order(order.id)
            logging.info("Cancelled all orders")
        except Exception as e:
            logging.error(f"Error cancelling all orders: {e}")

    def close_all_positions(self) -> None:
        """Close all open positions"""
        try:
            for position in self.open_positions:
                side = GenericOrderSide(OrderSideEnum.SELL if position.side == GenericPositionSide.LONG else OrderSideEnum.BUY)
                self.open_market_order(side, abs(position.size), is_reduce=True)
            logging.info("Closing all positions")
        except Exception as e:
            logging.error(f"Error closing all positions: {e}")

    async def critical_close_all(self) -> None:
        """Emergency close all positions and orders"""
        try:
            self.cancel_all_orders()
            await asyncio.sleep(2)
            self.close_all_positions()
            logging.warning("Critical close all executed")
        except Exception as e:
            logging.error(f"Error in critical close all: {e}")

    @property
    def balance(self) -> Decimal:
        return self._balance

    @balance.setter
    def balance(self, value: Decimal):
        self._balance = value

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
        for ws in self._ws_connections.values():
            if ws and not ws.closed:
                await ws.close()