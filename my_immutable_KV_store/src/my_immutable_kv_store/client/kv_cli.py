#!/usr/bin/env/python3

import cmd
import sys
import requests

from my_immutable_KV_store.src.my_immutable_kv_store.config.config import Config
# import logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)

from my_immutable_KV_store.src.my_immutable_kv_store.utils.profiling import timeit
from loguru import logger


class KVCli(cmd.Cmd):
    prompt = '> '
    keyspace = Config.DEFAULT_KEYSPACE

    @classmethod
    def do_use(cls, keyspace):
        cls.keyspace = keyspace

    @classmethod
    @timeit(level='WARNING')
    def do_get(cls, key):
        try:
            response = requests.get(f"http://localhost:{Config.MASTER_PORT}/get?key={key}&keyspace={cls.keyspace}").json()
            if response['status'] == Config.SUCCESS:
                logger.success(response['value'])
            else:
                logger.debug("None")
        except Exception as e:
            logger.critical(f"error retrieving key-value. Msg: {e.__str__()}")

    @staticmethod
    def do_quit(msg=None):
        logger.error("exiting ...")
        sys.exit(1)


if __name__ == '__main__':
    KVCli().cmdloop()
