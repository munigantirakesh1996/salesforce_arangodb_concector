
# === FILE: config/logger.py ===
import logging

def setup_logger():
    logger = logging.getLogger("sf_arango_connector")
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
