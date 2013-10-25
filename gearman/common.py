import struct
import asyncio
import logging

logger = logging.getLogger('gearman')

CAN_DO             = 1    # REQ    Worker
CANT_DO            = 2    # REQ    Worker
RESET_ABILITIES    = 3    # REQ    Worker
PRE_SLEEP          = 4    # REQ    Worker
# (unused)         = 5    # -      -
NOOP               = 6    # RES    Worker
SUBMIT_JOB         = 7    # REQ    Client
JOB_CREATED        = 8    # RES    Client
GRAB_JOB           = 9    # REQ    Worker
NO_JOB             = 10   # RES    Worker
JOB_ASSIGN         = 11   # RES    Worker
WORK_STATUS        = 12   # REQ    Worker
                          # RES    Client
WORK_COMPLETE      = 13   # REQ    Worker
                          # RES    Client
WORK_FAIL          = 14   # REQ    Worker
                          # RES    Client
GET_STATUS         = 15   # REQ    Client
ECHO_REQ           = 16   # REQ    Client/Worker
ECHO_RES           = 17   # RES    Client/Worker
SUBMIT_JOB_BG      = 18   # REQ    Client
ERROR              = 19   # RES    Client/Worker
STATUS_RES         = 20   # RES    Client
SUBMIT_JOB_HIGH    = 21   # REQ    Client
SET_CLIENT_ID      = 22   # REQ    Worker
CAN_DO_TIMEOUT     = 23   # REQ    Worker
ALL_YOURS          = 24   # REQ    Worker
WORK_EXCEPTION     = 25   # REQ    Worker
                          # RES    Client
OPTION_REQ         = 26   # REQ    Client/Worker
OPTION_RES         = 27   # RES    Client/Worker
WORK_DATA          = 28   # REQ    Worker
                          # RES    Client
WORK_WARNING       = 29   # REQ    Worker
                          # RES    Client
GRAB_JOB_UNIQ      = 30   # REQ    Worker
JOB_ASSIGN_UNIQ    = 31   # RES    Worker
SUBMIT_JOB_HIGH_BG = 32   # REQ    Client
SUBMIT_JOB_LOW     = 33   # REQ    Client
SUBMIT_JOB_LOW_BG  = 34   # REQ    Client
SUBMIT_JOB_SCHED   = 35   # REQ    Client
SUBMIT_JOB_EPOCH   = 36   # REQ    Client

COMMAND_NAMES = {
    CAN_DO             : 'CAN_DO',
    CANT_DO            : 'CANT_DO',
    RESET_ABILITIES    : 'RESET_ABILITIES',
    PRE_SLEEP          : 'PRE_SLEEP',

    NOOP               : 'NOOP',
    SUBMIT_JOB         : 'SUBMIT_JOB',
    JOB_CREATED        : 'JOB_CREATED',
    GRAB_JOB           : 'GRAB_JOB',
    NO_JOB             : 'NO_JOB',
    JOB_ASSIGN         : 'JOB_ASSIGN',
    WORK_STATUS        : 'WORK_STATUS',

    WORK_COMPLETE      : 'WORK_COMPLETE',

    WORK_FAIL          : 'WORK_FAIL',

    GET_STATUS         : 'GET_STATUS',
    ECHO_REQ           : 'ECHO_REQ',
    ECHO_RES           : 'ECHO_RES',
    SUBMIT_JOB_BG      : 'SUBMIT_JOB_BG',
    ERROR              : 'ERROR',
    STATUS_RES         : 'STATUS_RES',
    SUBMIT_JOB_HIGH    : 'SUBMIT_JOB_HIGH',
    SET_CLIENT_ID      : 'SET_CLIENT_ID',
    CAN_DO_TIMEOUT     : 'CAN_DO_TIMEOUT',
    ALL_YOURS          : 'ALL_YOURS',
    WORK_EXCEPTION     : 'WORK_EXCEPTION',

    OPTION_REQ         : 'OPTION_REQ',
    OPTION_RES         : 'OPTION_RES',
    WORK_DATA          : 'WORK_DATA',

    WORK_WARNING       : 'WORK_WARNING',

    GRAB_JOB_UNIQ      : 'GRAB_JOB_UNIQ',
    JOB_ASSIGN_UNIQ    : 'JOB_ASSIGN_UNIQ',
    SUBMIT_JOB_HIGH_BG : 'SUBMIT_JOB_HIGH_BG',
    SUBMIT_JOB_LOW     : 'SUBMIT_JOB_LOW',
    SUBMIT_JOB_LOW_BG  : 'SUBMIT_JOB_LOW_BG',
    SUBMIT_JOB_SCHED   : 'SUBMIT_JOB_SCHED',
    SUBMIT_JOB_EPOCH   : 'SUBMIT_JOB_EPOCH'
}

NULL_CHAR          = b'\x00'
MAGIC_REQ          = NULL_CHAR + b'REQ'
MAGIC_RES          = NULL_CHAR + b'RES'

PARAM_FOR_COMMAND  = {
    CAN_DO: ['func_name'],
    CANT_DO: ['func_name'],
    RESET_ABILITIES: [],
    PRE_SLEEP: [],
    NOOP: [],
    SUBMIT_JOB: ['func_name', 'unique', 'workload'],
    JOB_CREATED: ['job_handle'],
    GRAB_JOB: [],

    NO_JOB: [],
    JOB_ASSIGN: ['job_handle', 'func_name', 'workload'],
    WORK_STATUS: ['job_handle', 'numerator', 'denominator'],
    WORK_COMPLETE: ['job_handle', 'workload'],
    WORK_FAIL: ['job_handle'],
    GET_STATUS: ['job_handle'],
    ECHO_REQ: ['workload'],
    ECHO_RES: ['workload'],
    SUBMIT_JOB_BG: ['func_name', 'unique', 'workload'],
    ERROR: ['error_code', 'error_text'],

    STATUS_RES: ['job_handle', 'known', 'running', 'numerator', 'denominator'],
    SUBMIT_JOB_HIGH: ['func_name', 'unique', 'workload'],
    SET_CLIENT_ID: ['client_id'],
    CAN_DO_TIMEOUT: ['func_name', 'timeout'],
    ALL_YOURS: [],
    WORK_EXCEPTION: ['job_handle', 'workload'],
    OPTION_REQ: ['option_name'],
    OPTION_RES: ['option_name'],
    WORK_DATA: ['job_handle', 'workload'],
    WORK_WARNING: ['job_handle', 'workload'],

    GRAB_JOB_UNIQ: [],
    JOB_ASSIGN_UNIQ: ['job_handle', 'func_name', 'unique', 'workload'],
    SUBMIT_JOB_HIGH_BG: ['func_name', 'unique', 'workload'],
    SUBMIT_JOB_LOW: ['func_name', 'unique', 'workload'],
    SUBMIT_JOB_LOW_BG: ['func_name', 'unique', 'workload']
}

COMMAND_HEADER_SIZE = 12

class ProtocolError(Exception):
    pass

def to_bytes(string):
    if isinstance(string, str):
        return bytes(string, 'UTF8')
    else:
        string = b'' if string is None else string
        return string

def to_str(byte):
    if isinstance(byte, bytes):
        return str(byte, 'UTF8')
    else:
        return byte

def pack_binary_command(cmd_type, cmd_args={}, is_response=False):
    magic = MAGIC_RES if is_response else MAGIC_REQ
    excepted_cmd_params = PARAM_FOR_COMMAND.get(cmd_type, None)

    data_items = [cmd_args[param] for param in excepted_cmd_params]

    binary_payload = NULL_CHAR.join(map(to_bytes, data_items))
    payload_size = len(binary_payload)
    packing_format = '!4sII%ds' % payload_size

    return struct.pack(packing_format, magic, cmd_type, payload_size,
            binary_payload)

def parse_binary_command(in_buffer, is_response=True):
    in_buffer_size = len(in_buffer)
    magic = None
    cmd_type = None
    cmd_args = None
    cmd_len = 0
    excepted_packet_size = None

    if in_buffer_size < COMMAND_HEADER_SIZE:
        return cmd_type, cmd_args, cmd_len

    magic, cmd_type, cmd_len = struct.unpack('!4sII',
            in_buffer[:COMMAND_HEADER_SIZE])

    received_bad_response = is_response and bool(magic != MAGIC_RES)
    received_bad_request = not is_response and bool(magic != MAGIC_REQ)
    if received_bad_response or received_bad_request:
        raise ProtocolError('Malformed Magic')
    excepted_cmd_params = PARAM_FOR_COMMAND.get(cmd_type, None)
    if excepted_cmd_params is None:
        raise ProtocolError('Received unkonw binary command: %s' % cmd_type)

    excepted_packet_size = COMMAND_HEADER_SIZE + cmd_len

    if in_buffer_size < excepted_packet_size:
        return None, None, 0

    binary_payload = in_buffer[COMMAND_HEADER_SIZE:excepted_packet_size]
    split_arguments = []

    if len(excepted_cmd_params) > 0:
        split_arguments = binary_payload.split(NULL_CHAR,
                len(excepted_cmd_params) - 1)
    elif binary_payload:
        raise ProtocolError('Excepted no binary payload: %s' % cmd_type)

    if len(split_arguments) != len(excepted_cmd_params):
        raise ProtocolError('Received %d argument(s), excepting %d argument(s): %s'%\
                len(split_arguments), len(excepted_cmd_params), cmd_type)

    cmd_args = dict(zip(excepted_cmd_params, split_arguments))

    return cmd_type, cmd_args, excepted_packet_size

class BaseAgent(object):
    def __init__(self, reader, writer, extra = {}):
        self._reader = reader
        self._writer = writer
        self._buffer = []
        self._extra = extra

    @asyncio.coroutine
    def send(self, cmd_type, cmd_args={}, is_response = False):
        buf = pack_binary_command(cmd_type, cmd_args, is_response)
        self._writer.write(buf)
        logger.debug('Send[%s:%s]> CMD: %s | Buffer: %s'%(\
                self._extra['host'], self._extra['port'],
                COMMAND_NAMES.get(cmd_type), buf))
        yield from self._writer.drain()

    @asyncio.coroutine
    def read(self, is_response = True):
        buf = b''.join(self._buffer)
        buf += yield from self._reader.read(1024)
        while True:
            data = parse_binary_command(buf, is_response)
            if data[2] == 0:
                buf += yield from self._reader.read(1024)
            else:
                break

        self._buffer = [buf[data[2]:]]
        if data[0]:
            logger.debug('Recv[%s:%s]> CMD: %s | Buffer: %s'%(\
                    self._extra['host'], self._extra['port'],
                    COMMAND_NAMES.get(data[0]), buf[:data[2]]))
        return data[0], data[1]
