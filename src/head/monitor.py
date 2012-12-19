import threading, time, pickle, socket, conf, logging
from shared.packets import MasterHeartBeat, WorkerHeartBeat, HeartBeat,ProducerHeartBeat
from shared import AppState
from Queue import PriorityQueue

class VMMonitor(threading.Thread):
    def __init__(self):
        super(VMMonitor,self).__init__()
        self.daemon = True
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind((conf.HEAD_BIND_IP, conf.HEAD_PORT))
        self._lock = threading.RLock()
        self._hb_buffer = {}
        self._log = logging.getLogger("VMMonitor")


    def flush(self):
        """Empties the current heartbeat buffer and returns the old."""
        with self._lock:
            old_buffer = self._hb_buffer
            self._hb_buffer = {}
            return old_buffer
            
            

    def _add_to_buffer(self, addr, hb):
        """Put (new) packet in buffer per address, make sure it is sorted by
        timestamp, so even if it arrives later than a packet that was sent
        later, then it is still in order."""
        hb.recv_time = int(time.time())
        with self._lock:
            if addr not in self._hb_buffer:
                self._hb_buffer[addr] = PriorityQueue()
            self._hb_buffer[addr].put((hb.timestamp,hb))


    def run(self):
        self._log.info("TimeStamp\tType\tState\tCPU\t\tMEM\t\tDISK\tWorkers/Queue\tAddress")
        while True:
            data, addr = self._sock.recvfrom(1024) # Receive, 1024 bytes buffer.
            hb = pickle.loads(data) # Deserialize data into heart beat object.
            if not isinstance(hb,HeartBeat):
                continue

            self._add_to_buffer(addr, hb)

            if type(hb) is WorkerHeartBeat:
                self._log.info("%d\tworker\t%s\t%f\t%f\t%f\t%d-%f\t%s" %
                (hb.recv_time,hb.state, hb.cpu, hb.mem, hb.disk, hb.workers, hb.avg_time, addr[0] +"-"+ str(addr[1])))
            elif type(hb) is MasterHeartBeat:
                self._log.info( "%d\tmaster\t%s\t%f\t%f\t%f\t%d\t%s" % (hb.recv_time,hb.state, hb.cpu, hb.mem, hb.disk,hb.queue_length, addr[0] +"-"+ str(addr[1])))
            elif type(hb) is ProducerHeartBeat:
                self._log.info( "%d\tprdcer\t%s\t%f\t%f\t%f\t%s\t%s" % (hb.recv_time,hb.state, hb.cpu, hb.mem, hb.disk, "-", addr[0] +"-"+ str(addr[1])))

