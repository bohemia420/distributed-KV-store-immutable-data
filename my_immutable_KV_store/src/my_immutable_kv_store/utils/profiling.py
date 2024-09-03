# import logging
from loguru import logger
import time
from functools import wraps


def timeit(level="DEBUG"):
    logger_funcs = {
        "DEBUG": logger.debug,
        "WARNING": logger.warning
    }

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            logger_funcs[level](f"{func.__name__} executed in {elapsed_time:.4f} seconds")
            return result
        return wrapper
    return decorator
