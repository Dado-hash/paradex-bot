# Paradex Trading Bot (Market making strategy)

![codecov](https://codecov.io/gh/Ham1et/paradex-bot/branch/master/graph/badge.svg?token=E6XtGNKslf)
![GitHub Workflow Status](https://github.com/Ham1et/paradex-bot/actions/workflows/python-app.yml/badge.svg?branch=master)
![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Ham1et_paradex-bot&metric=sqale_rating&token=d4621e5830d7195cb9b8ad829049ce73773258ff)

#### [Link to Paradex](https://app.paradex.trade/r/cp0x1) | Referral code - cp0x1 | -10% commission with referral code | [Referral Dashboard](https://www.paradex.trade/stats)

[![Telegram](https://img.shields.io/badge/Telegram-Chat-blue?logo=telegram)](https://t.me/c/1639919522/30367)
[![Discord](https://img.shields.io/badge/Discord-Join-5865F2?logo=discord&logoColor=white)](https://discord.gg/paradex)

## ⚠️ Disclaimer

This software is for informational purposes only. It does not constitute financial advice, and users should conduct
their own research before executing any trading strategies.

### Initialize virtual environment

```cmd 
py -m venv venv
```

Windows `venv/Scripts/activate`

Linux/macOS `source venv/bin/activate`

```cmd 
pip install -r requirements.txt
```

### Running example

```cmd
env $(cat environments/parallel/sub-1-2/.env.parallel.BERA | sed '/^\s*#/d; /^\s*$/d; s/#.*//; s/\r//g' | xargs) python main.py
```

### Fix "Couldn't find libcrypto_c_exports" for Windows

https://github.com/software-mansion/starknet.py/discussions/1141

### Check ping to API server

Ping is the main parameter that will affect the bot's speed and, accordingly, its competitiveness with other bots.

For minimal ping, I recommend hosting the bot on servers located in Tokyo (AWS AP-NORTHEAST-1).

[Docx about Paradex server location](https://docs.paradex.trade/documentation/getting-started/architecture-overview/server-location)

```cmd
ping s3.ap-northeast-1.amazonaws.com
```

### Environments setup manual

#### Exchange | Paradex

Single

```dotenv
EXCHANGE_TYPE=1# 1 - Paradex | 2 - Backpack
L1_ADDRESS=# ETH public address
L2_PRIVATE_KEY=# Paradox private key for account/sub
```

Parallel

```dotenv
EXCHANGE_TYPE_1=1# 1 - Paradex | 2 - Backpack
L1_ADDRESS_1=# ETH public address
L2_PRIVATE_KEY_1=# Paradox private key for FIRST account/sub
EXCHANGE_TYPE_2=1# 1 - Paradex | 2 - Backpack
L1_ADDRESS_2=# ETH public address
L2_PRIVATE_KEY_2=# Paradox private key for SECOND account/sub
```

#### Exchange | Backpack

```dotenv
EXCHANGE_TYPE=2# 1 - Paradex | 2 - Backpack
API_KEY=
API_SECRET=
```

Parallel

```dotenv
EXCHANGE_TYPE_1=2# 1 - Paradex | 2 - Backpack
API_KEY_1=
API_SECRET_1=
EXCHANGE_TYPE_2=2# 1 - Paradex | 2 - Backpack
API_KEY_2=
API_SECRET_2=
```

#### Exchange | Parallel | Different

```dotenv
EXCHANGE_TYPE_1=2# 1 - Paradex | 2 - Backpack
API_KEY_1=
API_SECRET_1=
EXCHANGE_TYPE_2=2# 1 - Paradex | 2 - Backpack
L1_ADDRESS_2=# ETH public address
L2_PRIVATE_KEY_2=# Paradox private key for SECOND account/sub
```

#### General

```dotenv
IS_DEV=False/True# Prod/Testnet enviroment
LOG_FILE=False/True# Put it True if you want to save logs in file
PING_SECONDS=0.3# Ping from your server to paradex server in seconds(Check Ping section on top)
INITIAL_CLOSE_ALL_POSITIONS=False/True# Put it True if you want to close all postions and orders before start trading
```

#### Default for market | Paradex

```dotenv
MARKET=BERA-USD-PERP
PRICE_STEP=0.001
PRICE_ROUND=3# Number of decimal places for price step
SIZE_ROUND=0# Number of decimal places for order size step
MARKET_MIN_ORDER_SIZE=8# Minimal posible order size
MAX_LEVERAGE=5
```

#### Default for market | BackPack

```dotenv
BACKPACK_MARKET=BERA_USDC_PERP
BACKPACK_PRICE_STEP=0.0001
BACKPACK_PRICE_ROUND=4# Number of decimal places for price step
BACKPACK_MARKET_MIN_ORDER_SIZE=0.1# Number of decimal places for order size step
BACKPACK_SIZE_ROUND=1# Minimal posible order size
MAX_LEVERAGE=5
```

If you use different exchanges, you need to add "Default for market" for both

#### Params for analysis order price

```dotenv
DEFAULT_DEPTH_ORDER_BOOK_ANALYSIS=5# Number of first order in book for analysis
MAX_PRICE_STEPS_GAP=3# Max price steps gap between to order in book for place your order before
MIN_MARK_PRICE_PRICE_GAPS=3# Min number of price gap between order price and mark price
```

#### Individual params for each stratage

Single

```dotenv
STRATEGY_TYPE=1

MAX_POSITION_SIZE=30# Max full position on one side
MIN_ORDER_SIZE=15
MAX_ORDER_SIZE=15
MIN_POSITION_TIME_SECONDS=30# Min time for position life
MAX_TAKER_POSITION_TIME_SECONDS=600# After this TIME is over, position will close by MARKET
MAX_MAKER_POSITION_TIME_SECONDS=180# After this TIME is over, position will try close aggressive(first in order book) be LIMIT
TAKE_PROFIT_TAKER_THRESHOLD=0.02# If this profit percentage is reached, the position will be closed at MARKET
TAKE_PROFIT_MAKER_THRESHOLD=0.01# If this profit percentage is reached, the position will try close aggressive(first in order book) be LIMIT
STOP_LOSS_TAKER_THRESHOLD=0.05# If this lost percentage is reached, the position will be closed at MARKET
STOP_LOSS_MAKER_THRESHOLD=0.02# If this lost percentage is reached, the position will try close aggressive(first in order book) be LIMIT
```

Parallel

```dotenv
STRATEGY_TYPE=2

TRADING_MODE=1# 1 - Limit-Limit | 2 - Limit-Market
DEFAULT_ORDER_SIZE=15#Order size with will be use for open postions
POSITION_TIME_THRESHOLD_SECONDS=600#After this TIME is over, position will try to close
```

TRADING_MODE - 1 | The bot will try to open a position using a LIMIT order and close it also using a Limit order in both
accounts.

TRADING_MODE - 2 | The bot will try to open a position using a LIMIT order for both accounts, but if one account has an
open position, the other one will open a position using a MARKET order for hedging. For closing working in same way

### Recommendation

#### STRATEGY_TYPE=1 | Single

This strategy works best on **mid-liquidity** pairs with a **small-medium spread**. With low volatility and relatively
high trading volume, it can even trade at a profit, but during price spikes, it will incur losses.

Also, the more bots using this strategy on the same pair, the less effective it becomes.

**Best for**: Use this strategy if you want to accumulate **trading volume without holding** a position for a specific
time and
are not afraid of losses during price spikes.

#### STRATEGY_TYPE=2 | Parallel

The difference between this strategy and the first one is that it uses two accounts that will hedge each other.

##### TRADING_MODE=1 | Limit-Limit

It will **always** try to open and close positions with **LIMIT** orders.

Recommendations for pairs remain the same as in the first strategy: **mid-liquidity****, small-medium spread**.

The **risks** of **price spikes** remain the same, but **only** if the position was **opened** on just **one account**.

**Best for**: Use this strategy and mode if you want to apply the principles of the first strategy(**trading volume**)
but also aim to **hold positions** for a significant amount of time.

##### TRADING_MODE=2 | Limit-Market

**Initially**, it will try to **open/close** with a **LIMIT** order, but **if** there is a **difference** in position
**size**, it will **open/close** the position on the **other account** using a **Market** order.

In this strategy, the **only risk** of **price slippage** occurs when **balancing positions** using **MARKET** orders.

**Best for**: Use this strategy for pairs with **low volume** and **low open interest** since the costs will be nearly
the same for all paris, while **low-liquidity** pairs provide **more points**.

### PS

If you want to use this bot's trading strategies on other exchanges or to `STRATEGY_TYPE=2` on a different exchange for
hedge, you need to implement an abstract class BaseExchange and pass the required exchange when initializing the
strategy in `main()`.