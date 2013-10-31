import logging
import sys
import os
try:
    from asyncgear import Worker
except ImportError:
    sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
    from asyncgear import Worker
from asyncgear.common import logger

import asyncio
logger.setLevel(logging.DEBUG)
FORMAT = '%(asctime)-15s - %(message)s'
formater = logging.Formatter(FORMAT)
ch = logging.StreamHandler()
ch.setFormatter(formater)
logger.addHandler(ch)

def echo(job):
    print(job.workload)
    # yield from asyncio.sleep(10)
    yield from job.complete('cckk,haha')

def main():
    worker = Worker()
    # start the gearman server
    # sudo gearmand -d -p 4730
    # sudo gearmand -d -p 4731
    # sudo gearmand -d -p 4732
    yield from worker.add_server('localhost', 4730)
    yield from worker.add_server('localhost', 4731)
    yield from worker.add_server('localhost', 4732)
    yield from worker.add_func('echo', echo)
    worker.work()

loop = asyncio.get_event_loop()
task = asyncio.Task(main())
loop.run_forever()
