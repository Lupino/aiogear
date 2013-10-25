import asyncio
from . import common
import random

class ClientAgent(common.BaseAgent):
    level_normal = 'normal'
    level_low = 'low'
    level_high = 'high'

    @asyncio.coroutine
    def do(self, func_name, workload, unique = None, level = 'normal', background=False):
        payload = {
            'func_name': func_name,
            'unique': unique,
            'workload': workload
        }
        if background:
            if level == self.level_low:
                yield from self.send(common.SUBMIT_JOB_LOW_BG, payload)
            elif level == self.level_high:
                yield from self.send(common.SUBMIT_JOB_HIGH_BG, payload)
            else:
                yield from self.send(common.SUBMIT_JOB_BG, payload)
        else:
            if level == self.level_low:
                yield from self.send(common.SUBMIT_JOB_LOW, payload)
            elif level == self.level_high:
                yield from self.send(common.SUBMIT_JOB_HIGH, payload)
            else:
                yield from self.send(common.SUBMIT_JOB, payload)

        cmd_type, cmd_args = yield from self.read()

        if cmd_type == common.JOB_CREATED:
            job_handle = cmd_args['job_handle']

        if background:
            return job_handle
        else:
            return Task(self)

class Client(object):
    level_normal = ClientAgent.level_normal
    level_low = ClientAgent.level_low
    level_high = ClientAgent.level_high

    def __init__(self):
        self._agents = []

    @asyncio.coroutine
    def do(self, func_name, workload, unique = None, level = 'normal', background=False):
        agent = random.choice(self._agents)
        try:
            ret = yield from agent.do(func_name, workload, unique, level,
                    background)
        except ConnectionResetError as e:
            self._agents.remove(self._agents)
            raise e

        return ret

    @asyncio.coroutine
    def add_server(self, host, port, ssl = False):
        reader, writer = yield from asyncio.open_connection(host, port, ssl=ssl)
        agent = ClientAgent(reader, writer,
                {'host': host, 'port': port, 'ssl': ssl})
        self._agents.append(agent)


class Task(object):
    def __init__(self, agent):
        self._agent = agent

    @property
    def result(self):
        cmd_type, cmd_args = yield from self._agent.read()
        return cmd_type, cmd_args
