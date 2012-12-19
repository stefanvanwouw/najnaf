from shared import AppState
from master import TaskPoolMonitor, MasterController
from shared.resources import ResourceMonitor



def main():
    state = AppState()
    # Start monitor client.
    monitor = TaskPoolMonitor(state, ResourceMonitor())
    monitor.start()

    # Start controller.
    controller = MasterController(state)
    controller.run()

if __name__ == "__main__":
    main()
