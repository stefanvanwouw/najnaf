from shared import AppCommand, AppState
import socket, pickle, conf, time, subprocess

class MasterController(object):

    def __init__(self, state_obj):
        self._state_obj = state_obj
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((conf.CONTROLLER_BIND_IP, conf.CONTROLLER_PORT))
    
    def run(self):
        while True:
            #time.sleep(0.1) # unstress CPU
            data, addr = self._sock.recvfrom(1024) # Receive, 1024 bytes buffer.
            cmd = pickle.loads(data) # Deserialize data into command object.

            if not isinstance(cmd,AppCommand):
                continue
            else:
                if cmd.type == AppCommand.INIT and self._state_obj.state == AppState.READY:
                    self._state_obj.state = AppState.INIT
                    self.start()
                    self._state_obj.state = AppState.RUNNING
                elif cmd.type == AppCommand.SHUTDOWN and self._state_obj.state == AppState.RUNNING:
                    self._state_obj.state = AppState.SHUTTING_DOWN
                    self.stop()
                    self._state_obj.state = AppState.READY

    def start(self):
        proc = subprocess.Popen(["/etc/init.d/apollo-broker-service", "start"])
        proc.wait()

    def stop(self):
        # TODO: finish current connections. Could do this:
        #       If count(connections) == 0: Popen stop + self.state == AppState.READY. 
        #       Else do nothing and try again on receipt of new SHUTTING_DOWN command
        proc = subprocess.Popen(["/etc/init.d/apollo-broker-service", "stop"])
        proc.wait()
