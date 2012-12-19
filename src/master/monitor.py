import threading, time, pickle, socket
import conf
from shared.resources import ResourceMonitor
from shared.packets import MasterHeartBeat
from shared import AppState
import urllib2
import json

# TaskPoolMonitor
class TaskPoolMonitor(threading.Thread):
    
    def __init__(self, state_obj, resource_monitor):
        super(TaskPoolMonitor,self).__init__()
        self.daemon = True
        self.state_obj = state_obj
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.resource_monitor = resource_monitor
        self._apollo_api = ApolloApi()

    def run(self):
        while True:
            cpu = self.resource_monitor.cpu
            mem = self.resource_monitor.mem
            disk = self.resource_monitor.disk
            hb = MasterHeartBeat(self.state_obj,cpu,mem,disk, 0, 0)

            if self.state_obj.state == AppState.RUNNING:
                hb = MasterHeartBeat(self.state_obj,cpu,mem,disk, self._apollo_api.fetch_queue_size(), self._apollo_api.fetch_consumer_count())

            self.sock.sendto(pickle.dumps(hb, pickle.HIGHEST_PROTOCOL), (conf.HEAD_IP,
            conf.HEAD_PORT))

            time.sleep(conf.HEARTBEAT_INTERVAL)

class ApolloApi(object):

    def __init__(self):
        # create a password manager
        password_mgr = urllib2.HTTPPasswordMgrWithDefaultRealm()
        
        # Add the username and password.
        # If we knew the realm, we could use it instead of None.
        top_level_url = "http://localhost:" + str(conf.APOLLO_API_PORT) + "/"
        password_mgr.add_password(None, top_level_url, conf.APOLLO_USER, conf.APOLLO_PASSWORD)
        
        handler = urllib2.HTTPBasicAuthHandler(password_mgr)
        
        # create "opener" (OpenerDirector instance)
        opener = urllib2.build_opener(handler)
        
        # use the opener to fetch a URL
        urllib2.install_opener(opener)
    
    def fetch_data(self):
        a_url = "http://localhost:" + str(conf.APOLLO_API_PORT) + "/broker/queue-metrics.json"
        
        req = urllib2.Request(a_url)
        result = urllib2.urlopen(req)

        return result.read()

    def fetch_queue_size(self):
        data = json.loads(self.fetch_data())

        return int(data["queue_items"])

    def fetch_consumer_count(self):
        data = json.loads(self.fetch_data())

        return int(data["consumer_count"])
