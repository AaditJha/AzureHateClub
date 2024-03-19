import threading
from datetime import datetime

class ElectionTimer:
    def __init__(self, interval, function, debug=True):
        self.function = function
        self.debug = debug
        self.interval = interval
        self.timer = None

    def callback(self):
        self.function()
        self.start()
    
    def start(self,interval=None,reset=False):
        if self.timer is not None:
            self.timer.cancel()      
        del self.timer
        if interval is not None:
            self.interval = interval
        self.timer = threading.Timer(self.interval, self.callback)
        self.timer.start()
        if self.debug:
            if reset:
                print("Timer reset at ", datetime.now().strftime("%A, %B %d, %Y %I:%M:%S %p"))
            else:
                print("Timer started at ", datetime.now().strftime("%A, %B %d, %Y %I:%M:%S %p"), "with interval", self.interval, "seconds")

    def reset(self):
        self.timer.cancel()
        self.start(None,True)