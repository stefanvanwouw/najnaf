import socket, logging
import pickle
import conf

from shared.app import AppState
from worker import WorkerMonitor, WorkerController, Consumer
from shared.resources import ResourceMonitor


def main():
    logging.basicConfig(level=logging.DEBUG)
    state = AppState()
    # Start monitor client.
    resource_monitor = ResourceMonitor()

    consumer = Consumer(state, resource_monitor)

    # Start monitor client.
    monitor = WorkerMonitor(state, resource_monitor, consumer)
    monitor.start()

    consumer.set_monitor(monitor)

    controller = WorkerController(state, consumer)
    controller.run()



if __name__ == "__main__":
    main()
