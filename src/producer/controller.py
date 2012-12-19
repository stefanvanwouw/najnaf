from shared.app import AppState, AppCommand
from shared.packets import ProducerType
import socket, pickle, conf, time, threading, random, stomp

class ProducerThread(threading.Thread):
    def __init__(self, state, conn, type, frequency):
        super(ProducerThread,self).__init__()
        self.daemon = True
        self._conn = conn
        self._type = type
        self._frequency = frequency
        self._done = False
        self._state = state

    def run(self):
        while not self._done:
            if self._type.type == ProducerType.CONSTANT:
                count = self._frequency[0]
                interval = self._frequency[1]
                for i in range(1, count):
                    if not self.send(conf.PRODUCER_IMG_URL):
                        return

                time.sleep(interval)
            else:
                min_count = self._frequency[0]
                max_count = self._frequency[1]
                count = random.randint(min_count, max_count)
                interval = self._frequency[2]
                for i in range(1, count):
                    if not self.send(conf.PRODUCER_IMG_URL):
                        return

                time.sleep(interval)

    def notify(self):
        self._done = True

    def send(self, message):
        try:
            self._conn.send(message, destination=conf.APOLLO_DEST)
            return True
        except exception:
            self._done = True
            self._state.state = AppState.ERROR
            return False

class ProducerController(object):

    def __init__(self, state_obj):
        self._state_obj = state_obj
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((conf.CONTROLLER_BIND_IP, conf.CONTROLLER_PORT))

    def run(self):
        while True:
            data, addr = self._sock.recvfrom(1024) # Receive, 1024 bytes buffer.
            cmd = pickle.loads(data) # Deserialize data into command object.

            if not isinstance(cmd, AppCommand):
                continue
            else:
                if cmd.type == AppCommand.INIT and (self._state_obj.state == AppState.READY or self._state_obj.state == AppState.ERROR):
                    self._state_obj.state = AppState.INIT
                    data = cmd.data
                    ip = data[0]
                    type = data[1]
                    frequency = data[2]
                    conn = stomp.Connection(host_and_ports =
                        [ (ip, 61613) ],
                        user=conf.APOLLO_USER,
                        passcode=conf.APOLLO_PASSWORD
                    )
                    conn.start()
                    conn.connect()
                    self._state_obj.state = AppState.RUNNING
                    self._producer_thread = ProducerThread(self._state_obj, conn, type, frequency)
                    self._producer_thread.start()
                elif cmd.type == AppCommand.SHUTDOWN and self._state_obj.state == AppState.RUNNING:
                    self._state_obj.state = AppState.SHUTTING_DOWN
                    self._producer_thread.notify()
                    self._producer_thread.join()

                    # unsubscribe and join threads
                    conn.disconnect()
                    self._state_obj.state = AppState.READY
