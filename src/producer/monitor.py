import threading, time, pickle, socket
import conf
from shared.packets import ProducerHeartBeat

class ProducerMonitor(threading.Thread):
    def __init__(self, state_obj, resource_monitor):
        super(ProducerMonitor,self).__init__()
        self.daemon = True
        self._state_obj = state_obj
	self._resource_monitor = resource_monitor
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def run(self):
        while True:
            cpu = self._resource_monitor.cpu
            mem = self._resource_monitor.mem
            disk = self._resource_monitor.disk

            hb = ProducerHeartBeat(self._state_obj,cpu,mem,disk)
            self.sock.sendto(pickle.dumps(hb, pickle.HIGHEST_PROTOCOL), (conf.HEAD_IP,
            conf.HEAD_PORT))

            time.sleep(conf.HEARTBEAT_INTERVAL)
