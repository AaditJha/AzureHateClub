import threading
from datetime import datetime
import time

class ElectionTimer:
    def __init__(self, interval, function, is_election_timer = True, debug=True):
        self.function = function
        self.debug = debug
        self.interval = interval
        self.timer : threading.Timer = None
        self.start_time = None
        self.is_election_timer = is_election_timer

    def callback(self):
        self.function()
        self.start_time = None
    
    def cancel(self):
        if self.timer is not None:
            self.timer.cancel()
            self.start_time = None
    
    def get_timer(self):
        if self.start_time is not None:
            return max(0,self.interval - time.time() + self.start_time)
        return 0
    
    def start(self,interval=None,reset=False):
        if self.timer is not None:
            self.timer.cancel()      
        del self.timer
        if interval is not None:
            self.interval = interval
        self.timer = threading.Timer(self.interval, self.callback)
        self.start_time = time.time()
        self.timer.start()
        timer_type = "Election" if self.is_election_timer else "Lease"
        reset_str = "reset" if reset else "started"
        if self.debug:
            print(timer_type,"Timer",reset_str,"at", datetime.now().strftime("%I:%M:%S"), f": {self.interval :.2f}s")

    def reset(self):
        self.timer.cancel()
        self.start(None,True)