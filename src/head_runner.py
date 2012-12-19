import logging
from head import VMMonitor, VMScheduler
from head.vm_wrapper import VMType


def main():
    logging.basicConfig(level=logging.DEBUG)
    monitor = VMMonitor()
    monitor.start()


    controller = VMScheduler(monitor)
    controller.run()





if __name__ == "__main__":
    main()

