from shared.app import AppState, AppCommand
import socket, pickle, conf, time

class WorkerController(object):

    def __init__(self, state_obj, consumer):
        self._state_obj = state_obj
        self._consumer = consumer
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((conf.CONTROLLER_BIND_IP, conf.CONTROLLER_PORT))
        self._master_ip = None
    
    def run(self):
        while True:
            data, addr = self._sock.recvfrom(1024) # Receive, 1024 bytes buffer.
            cmd = pickle.loads(data) # Deserialize data into command object.

            if not isinstance(cmd,AppCommand):
                continue
            else:
                if cmd.type == AppCommand.INIT and (self._state_obj.state ==
                        AppState.READY or self._state_obj.state ==
                        AppState.ERROR or (self._state_obj.state ==
                            AppState.RUNNING and self._master_ip != cmd.data)):
                    print "received init"
                    self._state_obj.state = AppState.INIT
                    self._master_ip = cmd.data

                    try:
                        self._consumer.connect(self._master_ip, conf.APOLLO_QUEUE_PORT)
                        self._consumer.subscribe()
                        self._state_obj.state = AppState.RUNNING
                    except:
                        self._state_obj.state = AppState.ERROR
                elif cmd.type == AppCommand.SHUTDOWN and self._state_obj.state == AppState.RUNNING:
                    self._state_obj.state = AppState.SHUTTING_DOWN

                    # unsubscribe and join threads
                    self._consumer.shut_down()
