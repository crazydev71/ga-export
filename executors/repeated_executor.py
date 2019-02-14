# Repeated Executor

import time
from threading import Thread


class RepeatedExecutor(object):
    def __init__(self, interval, function, *args, **kwargs):
        self._timer = None
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running = False
        self.worker = Thread(target=self._run)
        # self.start()

    def _run(self):
        while self.is_running:
            self.function(*self.args, **self.kwargs)
            time.sleep(self.interval)

    def start(self):
        self.is_running = True
        self.worker.start()

    def stop(self):
        self.is_running = False
