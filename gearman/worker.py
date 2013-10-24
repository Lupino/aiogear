import asyncio
from . import common

class WorkerAgent(common.BaseAgent):

    def __init__(self, worker, reader, writer):
        common.BaseAgent.__init__(self, reader, writer)
        self._worker = worker
        self.is_running = False

    @asyncio.coroutine
    def add_func(self, func_name, timeout = 0):
        if timeout > 0:
            yield from self.send(common.CAN_DO_TIMEOUT,
                    {'func_name': func_name, 'timeout': timeout})
        else:
            yield from self.send(common.CAN_DO, {'func_name': func_name})

    @asyncio.coroutine
    def work(self):
        self.is_running = True
        yield from self.send(common.GRAB_JOB)
        cmd_type, cmd_args = yield from self.read()
        if cmd_type == common.NO_JOB:
            yield from self.send(common.PRE_SLEEP)
            yield from self.read()
        elif cmd_type == common.JOB_ASSIGN:
            func_name = common.to_str(cmd_args['func_name'])
            if self._worker.has_func(func_name):
                yield from self._worker.run_func(func_name, Job(self, cmd_args))
            else:
                yield from self.send(common.CANT_DO, {'func_name': cmd_args['func_name']})

    @asyncio.coroutine
    def set_client_id(self, client_id):
        yield from self.send(common.SET_CLIENT_ID, {'client_id': client_id})

    def finish(self):
        self.is_running = False

class Job(object):
    def __init__(self, agent, cmd_args):
        self._agent = agent
        for key, val in cmd_args.items():
            if key != 'workload':
                val = common.to_str(val)

            setattr(self, key, val)

        self.handle = self.job_handle

    @asyncio.coroutine
    def send(self, cmd_type, data=None):
        if data:
            yield from self._agent.send(cmd_type,
                {'job_handle': self.handle, 'workload': data})
        else:
            yield from self._agent.send(cmd_type, {'job_handle': self.handle})

    @asyncio.coroutine
    def complete(self, data):
        yield from self.send(common.WORK_COMPLETE, data)

    @asyncio.coroutine
    def fail(self):
        yield from self.send(common.WORK_FAIL)

    @asyncio.coroutine
    def status(self, numerator, denominator):
        yield from self._agent.send(common.WORK_STATUS, {
            'job_handle': self.handle,
            'numerator': numerator,
            'denominator': denominator
        })

    @asyncio.coroutine
    def data(self, data):
        yield from self.send(common.WORK_DATA, data)

    @asyncio.coroutine
    def warning(self, data):
        yield from self.send(common.WORK_WARNING, data)

    @asyncio.coroutine
    def exception(self, data):
        yield from self.send(common.WORK_EXCEPTION, data)

class Worker(object):
    def __init__(self, max_tasks=5):
        self._agents = []
        self._funcs = {}
        self._sem = asyncio.Semaphore(max_tasks)

    def work(self, max_tasks = 5):
        for agent in self._agents:
            if agent.is_runing:
                continue
            task = asyncio.Task(agent.work())
            task.add_done_callback(lambda t: agent.finish())
            task.add_done_callback(lambda t: self.work())

    @asyncio.coroutine
    def add_func(self, func_name, callback, timeout=0):
        self._funcs[func_name] = callback
        for agent in self._agents:
            yield from agent.add_func(func_name, timeout)

    def has_func(self, func_name):
        if func_name in self._funcs:
            return True
        return False

    @asyncio.coroutine
    def run_func(self, func_name, job):
        yield from self._sem.acquire()
        func = self._funcs[func_name]
        task = asyncio.Task(func(job))
        task.add_done_callback(lambda t: self._sem.release())

    @asyncio.coroutine
    def add_server(self, host, port, ssl = False):
        reader, writer = yield from asyncio.open_connection(host, port, ssl=ssl)
        agent = WorkerAgent(self, reader, writer)
        self._agents.append(agent)
