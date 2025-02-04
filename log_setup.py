import logging
import os
from datetime import datetime

import colorlog


def log_setup():
    LOG_FILE = os.getenv("LOG_FILE", "FALSE").lower() == "true"

    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={'DEBUG': 'blue', 'INFO': 'green', 'WARNING': 'yellow', 'ERROR': 'red', 'CRITICAL': 'bold_red'}
    )
    handlers = [logging.StreamHandler()]
    if LOG_FILE:
        log_filename = "/" + os.getenv("COIN") + "_" + datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")
        directory = "log/" + os.getenv("COIN") + "/PROD" if os.getenv("IS_DEV").lower() == "false" else "/TESTNET"
        if not os.path.exists(directory):
            os.makedirs(directory)
        handlers.append(logging.FileHandler(directory + log_filename))

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )
    logging.getLogger().handlers[0].setFormatter(formatter)
