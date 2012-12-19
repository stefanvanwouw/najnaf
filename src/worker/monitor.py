import threading, time, pickle, socket, math
import conf
from shared.resources import ResourceMonitor
from shared.packets import WorkerHeartBeat

class WorkerMonitor(threading.Thread):
    
    def __init__(self, state_obj, resource_monitor, consumer):
        super(WorkerMonitor,self).__init__()
        self.daemon = True
        self.state_obj = state_obj
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.resource_monitor = resource_monitor
        self._consumer = consumer
        self._message_processing_time = []
        self._thread_counts = []

    def run(self):
        while True:
            cpu = self.resource_monitor.cpu
            mem = self.resource_monitor.mem
            disk = self.resource_monitor.disk

            # add the newest thread_count to the list
            self._thread_counts.append(self._consumer.get_thread_count())
            self.shorten_list(self._thread_counts, conf.MAX_WORKER_THREAD_HISTORY)


            hb = WorkerHeartBeat(self.state_obj,cpu,mem,disk,
                int(math.ceil(self.average(self._thread_counts, 0))),
                self.average(self._message_processing_time, 0))
            self.sock.sendto(pickle.dumps(hb, pickle.HIGHEST_PROTOCOL), (conf.HEAD_IP,
            conf.HEAD_PORT))
	    print hb.state

            time.sleep(conf.HEARTBEAT_INTERVAL)

    def add_message_processing_time(self, time):
        self._message_processing_time.append(time)
        self.shorten_list(self._message_processing_time, conf.MAX_WORKER_AVERAGE)

    def shorten_list(self, list, limit):
        if len(list) > limit:
            list = list[-limit:]

    def average(self, list, default):
        return float(sum(list)) / len(list) if len(list) > 0 else default
