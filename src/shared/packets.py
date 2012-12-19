import threading
import time

class HeartBeat(object):
    def __init__(self, state, cpu, mem, disk):
        self.state = state
        self.cpu = cpu
        self.mem = mem
        self.disk = disk
        self.timestamp = int(time.time()) # We do only care about second precision
        self.recv_time = None

class WorkerHeartBeat(HeartBeat):

    def __init__(self, state, cpu, mem, disk, workers, avg_time):
        self.workers = workers
        self.avg_time = avg_time
        super(WorkerHeartBeat, self).__init__(state, cpu, mem, disk)

class MasterHeartBeat(HeartBeat):

    def __init__(self, state, cpu, mem, disk, queue_length, consumer_count):
        self.queue_length = queue_length
        self.consumer_count = consumer_count 
        super(MasterHeartBeat, self).__init__(state, cpu, mem, disk)

class ProducerHeartBeat(HeartBeat):
    pass

class ProducerType(object):
    CONSTANT = 0
    RANDOM = 1

    def __init__(self, type = CONSTANT):
        if type != ProducerType.CONSTANT and type != RANDOM:
            print "invalid type initialized"
        self.type = type

    def _set_type(self, type):
        self._type= type

    def _get_type(self):
        return self._type

    type = property(_get_type, _set_type)
