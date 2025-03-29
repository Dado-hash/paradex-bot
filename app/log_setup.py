import logging
import os
from datetime import datetime

import colorlog


def get_market():
    if os.getenv("MARKET"):
        return os.getenv("MARKET")
    elif os.getenv("BACKPACK_MARKET"):
        return os.getenv("BACKPACK_MARKET")


def log_setup():
    LOG_FILE = os.getenv("LOG_FILE", "FALSE").lower() == "true"
    MARKET_LOG = get_market()

    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={'DEBUG': 'blue', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'bold_red'}
    )
    handlers = [logging.StreamHandler()]
    if LOG_FILE:
        log_filename = "/" + MARKET_LOG + "_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
        directory = "log/" + MARKET_LOG + ("/PROD" if os.getenv("IS_DEV").lower() == "false" else "/TESTNET")
        if not os.path.exists(directory):
            os.makedirs(directory)
        handlers.append(logging.FileHandler(directory + log_filename))

    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    logging.getLogger().handlers[0].setFormatter(formatter)
