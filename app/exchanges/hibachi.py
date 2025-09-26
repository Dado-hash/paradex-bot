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
from app.models.generic_position_side import GenericPositionSide, PositionSideEnum


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
        """Initialize WebSocket data streams for Hibachi (REST disabled to avoid rate limits)"""
        try:
            logging.info("Initializing Hibachi WebSocket-only data streams...")

            # Wait a bit to avoid initial rate limits
            await asyncio.sleep(2)

            # Start WebSocket connections only
            market_task = asyncio.create_task(self._connect_market_ws())
            
            # Wait a bit more before account WebSocket to stagger connections
            await asyncio.sleep(1)
            account_task = asyncio.create_task(self._connect_account_ws())
            
            # Only get initial market data once, then rely on WebSocket
            # asyncio.create_task(self._get_initial_market_data())
            
            # Disable REST data loop to avoid rate limits
            # asyncio.create_task(self.__init_data_loop())

            logging.info("Hibachi WebSocket-only data streams initialized")

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
        """Connect to market data WebSocket based on Hibachi API documentation"""
        market_symbol = os.getenv("HIBACHI_MARKET", "ETH/USDT-P")
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # WebSocket URL from documentation - use market data endpoint
                ws_url = f"{self.ws_url}/ws/market"
                logging.info(f"Connecting to Hibachi Market WebSocket: {ws_url} (attempt {retry_count + 1})")
                logging.info(f"Market symbol: {market_symbol}")
                
                async with websockets.connect(
                    ws_url, 
                    timeout=30,
                    ping_interval=20,
                    ping_timeout=10
                ) as websocket:
                    self._ws_connections['market'] = websocket
                    logging.info(f"✅ Market WebSocket connected successfully")

                    # Subscribe to market data - based on Postman documentation
                    # Subscribe to price updates
                    subscribe_price_msg = {
                        "method": "subscribe", 
                        "parameters": {
                            "subscriptions": [
                                {
                                    "symbol": market_symbol,
                                    "topic": "mark_price"
                                },
                                {
                                    "symbol": market_symbol, 
                                    "topic": "spot_price"
                                },
                                {
                                    "symbol": market_symbol,
                                    "topic": "ask_bid_price"
                                },
                                {
                                    "symbol": market_symbol,
                                    "topic": "funding_rate_estimation"
                                }
                            ]
                        }
                    }
                    
                    await websocket.send(json.dumps(subscribe_price_msg))
                    logging.info(f"Subscribed to market data for {market_symbol}")

                    # Subscribe to orderbook updates
                    subscribe_orderbook_msg = {
                        "method": "subscribe",
                        "parameters": {
                            "subscriptions": [
                                {
                                    "symbol": market_symbol,
                                    "topic": "orderbook"
                                }
                            ]
                        }
                    }
                    
                    await websocket.send(json.dumps(subscribe_orderbook_msg))
                    logging.info(f"Subscribed to orderbook for {market_symbol}")
                    logging.debug(f"Orderbook subscription message: {subscribe_orderbook_msg}")

                    # Reset retry count on successful connection
                    retry_count = 0

                    # Listen for messages
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            logging.debug(f"Received market WebSocket message: {data}")
                            await self._handle_hibachi_market_data(data)
                        except Exception as e:
                            logging.error(f"Error processing WebSocket market data: {e}")
                            continue
                            
                    # If we reach here, connection was closed
                    logging.warning("WebSocket connection closed, attempting to reconnect...")

            except Exception as e:
                retry_count += 1
                logging.error(f"WebSocket connection error (attempt {retry_count}): {e}")
                if retry_count < max_retries:
                    wait_time = min(5 * retry_count, 30)  # Exponential backoff, max 30s
                    logging.info(f"Retrying in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error("Max WebSocket retries reached, activating REST fallback")
                    # Activate REST fallback only when WebSocket completely fails
                    asyncio.create_task(self._rest_fallback_mode())
                    break

        logging.warning("Market WebSocket connection attempts exhausted")

    async def _handle_hibachi_market_data(self, data: Dict[Any, Any]):
        """Handle market data from Hibachi WebSocket"""
        try:
            logging.debug(f"Processing market data: {data}")
            
            # Check for error messages first
            if 'error' in data:
                logging.error(f"Market WebSocket error: {data['error']}")
                return
            
            # Check for success/confirmation messages
            if 'result' in data:
                logging.info(f"Market WebSocket result: {data['result']}")
                return
            
            # Handle different message types based on Hibachi WebSocket format
            if 'topic' in data:
                topic = data.get('topic')
                symbol = data.get('symbol', '')
                logging.debug(f"Processing topic: {topic}, symbol: {symbol}")
                
                if topic == 'mark_price':
                    # Update mark price
                    mark_price = data.get('mark_price')
                    if mark_price:
                        self.mark_price = Decimal(str(mark_price))
                        logging.debug(f"Updated mark price: {self.mark_price}")
                
                elif topic == 'ask_bid_price':
                    # Update best bid/ask
                    ask_price = data.get('ask_price')
                    bid_price = data.get('bid_price')
                    
                    if bid_price and ask_price:
                        # Simple order book with just best bid/ask
                        self.buy_orders_list = [(Decimal(str(bid_price)), Decimal('1.0'))]
                        self.sell_orders_list = [(Decimal(str(ask_price)), Decimal('1.0'))]
                        self.mark_price = (Decimal(str(bid_price)) + Decimal(str(ask_price))) / 2
                        logging.debug(f"Updated bid/ask: {bid_price}/{ask_price}")
                
                elif topic == 'orderbook':
                    # Hibachi orderbook format
                    logging.info(f"Raw orderbook message: {data}")
                    
                    # Extract data from Hibachi format
                    orderbook_data = data.get('data', {})
                    bid_data = orderbook_data.get('bid', {})
                    ask_data = orderbook_data.get('ask', {})
                    
                    # Get levels from bid/ask data
                    bid_levels = bid_data.get('levels', [])
                    ask_levels = ask_data.get('levels', [])
                    
                    logging.info(f"Hibachi orderbook - Bid levels: {len(bid_levels)}, Ask levels: {len(ask_levels)}")
                    
                    # If levels are empty, use start/end prices as fallback
                    if not bid_levels and not ask_levels:
                        bid_start = bid_data.get('startPrice')
                        bid_end = bid_data.get('endPrice')
                        ask_start = ask_data.get('startPrice')
                        ask_end = ask_data.get('endPrice')
                        
                        logging.info(f"Using price ranges - Bid: {bid_end}-{bid_start}, Ask: {ask_start}-{ask_end}")
                        
                        # Create orderbook from price ranges
                        if bid_start and ask_start:
                            # Use the start prices as best bid/ask
                            self.buy_orders_list = [(Decimal(str(bid_start)), Decimal('1.0'))]
                            self.sell_orders_list = [(Decimal(str(ask_start)), Decimal('1.0'))]
                            
                            # Update mark price
                            best_bid = Decimal(str(bid_start))
                            best_ask = Decimal(str(ask_start))
                            self.mark_price = (best_bid + best_ask) / 2
                            
                            logging.info(f"Updated orderbook from ranges - Bid: {best_bid}, Ask: {best_ask}, Mark: {self.mark_price}")
                    else:
                        # Process levels if they exist
                        if bid_levels:
                            self.buy_orders_list = [
                                (Decimal(str(level['price'])), Decimal(str(level['quantity'])))
                                for level in bid_levels[:10]  # Top 10 levels
                            ]
                        
                        if ask_levels:
                            self.sell_orders_list = [
                                (Decimal(str(level['price'])), Decimal(str(level['quantity'])))
                                for level in ask_levels[:10]  # Top 10 levels
                            ]
                        
                        # Update mark price from levels
                        if self.buy_orders_list and self.sell_orders_list:
                            best_bid = self.buy_orders_list[0][0]
                            best_ask = self.sell_orders_list[0][0] 
                            self.mark_price = (best_bid + best_ask) / 2
                            logging.info(f"Updated orderbook from levels - {len(bid_levels)} bids, {len(ask_levels)} asks")
            
            elif 'messageType' in data and data['messageType'] == 'Snapshot':
                # Handle snapshot message format
                logging.info("Received market data snapshot")
            else:
                # Unknown message format - log everything to understand structure
                logging.warning(f"Unknown message format received: {data}")
                if isinstance(data, dict):
                    for key, value in data.items():
                        logging.info(f"Message field: {key} = {value}")
                
        except Exception as e:
            logging.error(f"Error handling market data: {e}")
            logging.debug(f"Raw message: {data}")

    async def _connect_account_ws(self):
        """Connect to account data WebSocket based on Hibachi API documentation"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Get account ID from environment
                account_id = os.getenv("HIBACHI_ACCOUNT_ID")
                if not account_id:
                    logging.error("HIBACHI_ACCOUNT_ID not set in environment")
                    return
                
                # Account WebSocket URL with accountId parameter as shown in docs
                ws_url = f"{self.trade_base_url.replace('https', 'wss')}/ws/account?accountId={account_id}"
                
                # Headers for authentication - use API key directly as shown in docs
                headers = {}
                if self.api_key:
                    headers["Authorization"] = self.api_key
                
                logging.info(f"Connecting to Hibachi Account WebSocket: {ws_url} (attempt {retry_count + 1})")
                
                async with websockets.connect(
                    ws_url, 
                    extra_headers=headers,
                    timeout=30,
                    ping_interval=20,
                    ping_timeout=10
                ) as websocket:
                    self._ws_connections['account'] = websocket
                    logging.info(f"✅ Account WebSocket connected successfully")

                    # First send stream.start as per documentation
                    stream_start_msg = {
                        "id": 123,
                        "method": "stream.start",
                        "params": {
                            "accountId": int(account_id)
                        }
                    }
                    
                    await websocket.send(json.dumps(stream_start_msg))
                    logging.info(f"Sent stream.start for account {account_id}")
                    
                    # Wait for stream.start response before proceeding
                    response = await websocket.recv()
                    response_data = json.loads(response)
                    logging.info(f"Stream.start response: {response_data}")

                    # Reset retry count on successful connection
                    retry_count = 0

                    # Listen for messages
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            await self._handle_hibachi_account_data(data)
                        except Exception as e:
                            logging.error(f"Error processing WebSocket account data: {e}")
                            continue
                            
                    # If we reach here, connection was closed
                    logging.warning("Account WebSocket connection closed, attempting to reconnect...")

            except Exception as e:
                retry_count += 1
                logging.error(f"Account WebSocket connection error (attempt {retry_count}): {e}")
                if retry_count < max_retries:
                    wait_time = min(5 * retry_count, 30)  # Exponential backoff, max 30s
                    logging.info(f"Retrying account WebSocket in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logging.error("Max account WebSocket retries reached")
                    break

        logging.warning("Account WebSocket connection attempts exhausted")

    async def _handle_hibachi_account_data(self, data: Dict[Any, Any]):
        """Handle account data from Hibachi WebSocket"""
        try:
            # Check for error messages first
            if 'error' in data:
                logging.error(f"Account WebSocket error: {data['error']}")
                return
            
            # Check for success/confirmation messages
            if 'result' in data:
                logging.info(f"Account WebSocket result: {data['result']}")
                
                if 'accountSnapshot' in data['result']:
                    # Handle the stream.start response
                    account_snapshot = data['result']['accountSnapshot']
                    balance = account_snapshot.get('balance')
                    if balance:
                        self._balance = Decimal(str(balance))
                        logging.info(f"✅ Updated Hibachi balance from snapshot: {self._balance}")
                
                return
            
            # Handle different message types based on Hibachi WebSocket format
            if 'topic' in data:
                topic = data.get('topic')
                
                if topic == 'balance':
                    # Update balance
                    balance = data.get('balance')
                    if balance is not None:
                        self._balance = Decimal(str(balance))
                        logging.debug(f"Updated balance: {self._balance}")
                
                elif topic == 'orders':
                    # Update orders from WebSocket
                    orders = data.get('orders', [])
                    if orders:
                        self._update_orders({'orders': orders})
                        logging.debug(f"Updated {len(orders)} orders from WebSocket")
                
                elif topic == 'positions':
                    # Update positions from WebSocket
                    positions = data.get('positions', [])
                    if positions:
                        self._update_positions({'positions': positions})
                        logging.debug(f"Updated {len(positions)} positions from WebSocket")
                        
        except Exception as e:
            logging.error(f"Error handling account data: {e}")
            logging.debug(f"Raw account message: {data}")

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

    async def _rest_fallback_mode(self):
        """Emergency REST fallback when WebSocket completely fails"""
        logging.warning("⚠️ Entering REST fallback mode due to WebSocket failures")
        
        while True:
            try:
                # Much slower polling to avoid rate limits
                await asyncio.sleep(30)  # 30 second intervals
                
                market = os.getenv("HIBACHI_MARKET", "ETH/USDT-P")

                # Get minimal market data
                try:
                    depth_data = await self._make_request("GET", "/market/data/orderbook",
                                                        params={"symbol": market, "limit": 5}, auth=False)
                    if depth_data and 'bids' in depth_data and 'asks' in depth_data:
                        self.buy_orders_list = [
                            (Decimal(str(bid[0])), Decimal(str(bid[1])))
                            for bid in depth_data['bids'][:5]
                        ]
                        self.sell_orders_list = [
                            (Decimal(str(ask[0])), Decimal(str(ask[1])))
                            for ask in depth_data['asks'][:5]
                        ]

                        if self.buy_orders_list and self.sell_orders_list:
                            best_bid = self.buy_orders_list[0][0]
                            best_ask = self.sell_orders_list[0][0]
                            self.mark_price = (best_bid + best_ask) / 2
                            
                        logging.debug("REST fallback: Updated market data")
                except Exception as e:
                    logging.error(f"REST fallback market data error: {e}")
                
                # Try to reconnect WebSocket periodically
                if len(self._ws_connections) == 0:
                    logging.info("Attempting to reconnect WebSocket from fallback mode...")
                    asyncio.create_task(self._connect_market_ws())
                    break  # Exit fallback mode to try WebSocket again
                    
            except Exception as e:
                logging.error(f"Error in REST fallback mode: {e}")
                await asyncio.sleep(10)

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
                    side=GenericOrderSide(OrderSideEnum.BUY if order.get('side', '').upper() == 'BUY' else OrderSideEnum.SELL, self.exchange_type)
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
                    side_enum = PositionSideEnum.LONG if direction == 'Long' else PositionSideEnum.SHORT
                    side = GenericPositionSide(side_enum, self.exchange_type)

                    # For short positions, represent size as negative for consistency
                    size = quantity if direction == 'Long' else -quantity

                    data_position = DataPosition(
                        id=str(position.get('positionId', '')),
                        market=position.get('symbol', ''),
                        size=size,
                        side=side,
                        average_entry_price=entry_price
                    )
                    self.open_positions.append(data_position)
            except Exception as e:
                logging.error(f"Error parsing Hibachi position: {e}")

    async def cleanup(self):
        """Cleanup connections and tasks"""
        try:
            # Close WebSocket connections
            for name, ws in self._ws_connections.items():
                if ws and not ws.closed:
                    logging.info(f"Closing {name} WebSocket connection")
                    await ws.close()
            
            self._ws_connections.clear()
            
            # Close HTTP session
            if self._session and not self._session.closed:
                await self._session.close()
                
            logging.info("Hibachi cleanup completed")
        except Exception as e:
            logging.error(f"Error during cleanup: {e}")

    async def _update_orders_from_ws(self, data):
        """Update orders from WebSocket data - placeholder for future implementation"""
        pass

    async def _update_positions_from_ws(self, data):
        """Update positions from WebSocket data - placeholder for future implementation"""
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
            return None  # For now return None, as async task can't be returned from sync method

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
            return None  # For now return None, as async task can't be returned from sync method

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
                # Use the built-in method to get the opposite order side
                opposite_side = position.side.opposite_order_side()
                self.open_market_order(opposite_side, abs(position.size), is_reduce=True)
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