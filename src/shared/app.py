import threading
from abc import ABCMeta

class Command(object):
    __metaclass__ = ABCMeta

    def __init__(self, type, data):
        self._type = type
        self._data = data

    def _get_data(self):
        return self._data

    def _get_type(self):
        return self._type

    data = property(_get_data)
    type = property(_get_type)

class AppCommand(Command):
    INIT = 0
    SHUTDOWN = 1

    def __str__(self):
        return "(%s,%s)" % ({self.INIT: 'INIT',
                self.SHUTDOWN: 'SHUTDOWN'}[self._type] , str(self._data))


class AppState(object):

    # States when VM is running (states of our system).
    READY = 0           # Initial state when created on Master/Worker VMs
    INIT = 1    # State when given INIT command.
    RUNNING = 2         # State when done initializing.
    SHUTTING_DOWN = 3   # State when given SHUTDOWN command.
    ERROR = 4           # An error occured, error msg in err_msg
    

    def __init__(self, state=READY, err_msg=None):
        self._lock = threading.RLock()
        self._set_state(state)
        self._err_msg = err_msg
   
    def _get_err_msg(self):
        return self._err_msg
    err_msg = property(_get_err_msg)

    def _set_state(self, state):
        with self._lock:
            self._state = state
            if self._state != self.ERROR:
                self._err_msg = None

    def _get_state(self):
        with self._lock:
            return self._state

    def __getstate__(self):
        odict = self.__dict__.copy()
        del odict['_lock']
        return odict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self._lock = threading.RLock()

    state = property(_get_state, _set_state)

    def __str__(self):
        return {self.READY: 'READY',
                self.INIT: 'INIT ',
                self.RUNNING: 'RUNN ',
                self.SHUTTING_DOWN: 'SHUT ',
                self.ERROR: 'ERROR'}[self._state]
