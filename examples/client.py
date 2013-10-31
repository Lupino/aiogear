import logging
import sys
import os
try:
    from aiogear import Client
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from aiogear import Client
from aiogear.common import logger

import asyncio
logger.setLevel(logging.DEBUG)
FORMAT = '%(asctime)-15s - %(message)s'
formater = logging.Formatter(FORMAT)
ch = logging.StreamHandler()
ch.setFormatter(formater)
logger.addHandler(ch)

def main():
    client = Client()
    # start the gearman server
    # sudo gearmand -d -p 4730
    # sudo gearmand -d -p 4731
    # sudo gearmand -d -p 4732
    yield from client.add_server('localhost', 4730)
    yield from client.add_server('localhost', 4731)
    yield from client.add_server('localhost', 4732)
    for i in range(1000):
        task = yield from client.do('echo', b'ffddkk', background=False)
        result = yield from task.result
        print(result)

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
