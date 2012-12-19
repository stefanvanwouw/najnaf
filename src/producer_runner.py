from shared import AppState
from producer import ProducerController, ProducerMonitor
from shared.resources import ResourceMonitor

def main():
    state = AppState()

    # Start monitor client.
    resource_monitor = ResourceMonitor()
    monitor = ProducerMonitor(state, resource_monitor)
    monitor.start()

    # Start controller.
    controller = ProducerController(state)
    controller.run()

if __name__ == "__main__":
    main()
