import download
import conf
import logging
import math
import stomp
import socket
from shared.app import AppState
import sys
import time
from threading import Thread
import threading
import socket
import exceptions


# A custom class is used to we can check isinstance and only join MessageThreads
class TaskThread(threading.Thread):
    def __init__(self, consumer, headers, message, conn):
        super(TaskThread,self).__init__()
        self._headers = headers
        self._message = message
        self._consumer = consumer
        self._conn = conn

    def run(self):
        start = time.time()
        asdf = 0
        # TODO: renable this for real image resizing, we used the busy wait for experiments. download.downloadAndResize(self._message, self._headers["message-id"])
        busy_wait_time = 3
        try:
            busy_wait_time = int(self._message)
        except exceptions.ValueError:
            busy_wait_time = 3

        while int(time.time()) - int(start) < busy_wait_time:
            asdf = asdf + 1

        end = time.time()
        try:
            self._conn.ack(self._headers)
        except:
            pass
        finally:
            self._consumer.notify(self, end - start)

class SlowStartState(object):

    # States for slow start algorithm.
    EXP_START = 0               # on each ack: increase thread count by one
    EXP_TRESHOLD = 1              # once resource limit is reached, exp growth untill new max
    CONGESTION_AVOIDANCE = 2    # linear increase - after limit is reached

class Consumer(object):
    def __init__(self, state, resource_monitor):
        super(Consumer,self).__init__()
        self._state = state
        self._resource_monitor = resource_monitor
        self._thread_list = []
        self._lock = threading.RLock()

        self._log = logging.getLogger('consumer')

        self._slow_start_state = SlowStartState.EXP_START
        self._initial_max_threads = 1
        self._cwnd = 1
        self._sstresh = None
        self._increment = 0
        self.subscribed = False

    def set_monitor(self, monitor):
        self._monitor = monitor

    def subscribe(self):
	    self._conn.subscribe(headers={'credit': conf.WORKER_CREDIT}, destination=conf.APOLLO_DEST, ack='client-individual')
	    self.subscribed = True

    def unsubscribe(self):
        self._conn.unsubscribe(destination=conf.APOLLO_DEST)
        self.subscribed = False

    def shut_down(self):
        if self._state.state == AppState.SHUTTING_DOWN:
            self._log.debug('in suspending state...')
            if (self.subscribed == True):
                self._log.debug('unsubscribing...')
                try:
                    self._conn.unsubscribe(destination=conf.APOLLO_DEST)
                    self._conn.stop()
                except:
                    raise
                self.subscribed = False

            self._state.state = AppState.READY
        #else: exception (invalid state)

    def on_error(self, headers, message):
        self.error()
        print 'received an error %s' % message

    def on_disconnected(self):
        if not self._state.state == AppState.SHUTTING_DOWN and not self._state.state == AppState.READY:
            self.error()

    def error(self):
        self._state.state = AppState.ERROR
        with self._lock:
            for thread in self._thread_list:
                thread.join()
                self._thread_list.remove(thread)


    def notify(self, thread, duration):
        with self._lock:
            self._monitor.add_message_processing_time(duration)
            # in start state: increment cwnd when the thread list is full
            if self._slow_start_state == SlowStartState.EXP_START:
                if self.get_thread_count() == self._cwnd:
                    print "IF INCREMENTING cwnd FROM %d TO %d" % (self._cwnd,
                            self._cwnd + 1)
                    self._cwnd = self._cwnd + 1

            # in threshold state: increment cwnd when the thread list is full
            # change to CONGESTION_AVOIDANCE when cwnd reaches the treshold
            elif self._slow_start_state == SlowStartState.EXP_TRESHOLD:
                if self.get_thread_count() == self._cwnd:
                    print "ELIF INCREMENTING cwnd FROM %d TO %d" % (self._cwnd,
                            self._cwnd + 1)
                    self._cwnd = self._cwnd + 1

                    if self._cwnd == self._sstresh:
                        print "into CONGESTION_AVOIDANCE STATE"
                        self._slow_start_state = SlowStartState.CONGESTION_AVOIDANCE
                        self._increment = 0

            # in avoidance state: count the acks, and increase cwnd when the
            # thread list is full and more acks than cwnd have been encountered
            else:
                self._increment = self._increment + 1
                print "self.increment = %d self.cwnd = %d" % (self._increment,
                        self._cwnd)
                if self._increment >= self._cwnd and self.get_thread_count() == self._cwnd:
                    print "ELSE INCREMENTING cwnd FROM %d TO %d" % (self._cwnd,
                        self._cwnd + 1)
                    self._cwnd = self._cwnd + 1
                    self._increment = 0

            # remove the thread from the list
            self._thread_list.remove(thread)

    def on_message(self, headers, message):

        # Only process the message when the thread limit hasn't been reached yet
        while len(self._thread_list) >= self._cwnd:
            # lets the controllor know that a message is received but unprocessed
            print "cwnd = %d and thread_list size = %d" % (self._cwnd,
                    len(self._thread_list))
            self._in_hold = True
            time.sleep(conf.HEARTBEAT_INTERVAL)

        # resource limit is hit - reduce the `window size' and jump to the
        # correct state
        if not self._has_resources():
            with self._lock:
                print "HITTING RESOURCE LIMIT"
                if self._slow_start_state == SlowStartState.EXP_START:
                    print "FROM START TO TRESHHOLD"
                    self._slow_start_state = SlowStartState.EXP_TRESHOLD

                elif self._slow_start_state == SlowStartState.CONGESTION_AVOIDANCE:
                    print "FROM AVOIDANCE TO TRESHHOLD"
                    self._increment = 0

                self._sstresh = math.ceil(self._cwnd / 2.0)
                self._cwnd = self._initial_max_threads

        # create a thread for the message. To reduce complexity this is also
        # done when there aren't enough resources
        self.create_thread_for_message(headers, message)

        # Set to false to let the controllor know that no new messages are in
        # hold
        self._in_hold = False
        print "THREAD COUNT = %d" % (self.get_thread_count())

    def create_thread_for_message(self, headers, message):
        thread = TaskThread(self, headers, message, self._conn)
        self._thread_list.append(thread)
        thread.start()

    def get_thread_count(self):
        with self._lock:
            return len(self._thread_list)

    def get_window_size(self):
        return self._sstresh if self._sstresh is not None and self._sstresh > self._cwnd else self._cwnd

    # def on_connecting(self, host_and_port):
    #     self.subscribe()
    #     self._state.state = AppState.RUNNING

    def connect(self, ip, port):
        try:
            self._conn = stomp.Connection(host_and_ports = [ (ip, port) ], user=conf.APOLLO_USER, passcode=conf.APOLLO_PASSWORD)
            self._conn.set_listener('', self)
            self._conn.start()
            self._conn.connect()
        except stomp.exception.ConnectFailedException:
            self.error()

    def _has_resources(self):
        print "cpu = %f mem = %f" % (self._resource_monitor.cpu, self._resource_monitor.mem)
        return self._resource_monitor.cpu < 0.8 #and self._resource_monitor.mem < 0.8
