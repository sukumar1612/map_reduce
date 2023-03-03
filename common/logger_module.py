import logging
import os

import dotenv

dotenv.load_dotenv()


def get_logger(mod_name):
    level = logging.DEBUG if os.getenv("LOGGING_LEVEL") == "DEBUG" else logging.WARNING
    logger = logging.getLogger(mod_name)
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s [%(name)-s] %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger
