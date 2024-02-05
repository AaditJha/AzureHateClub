import pickle

class ServerStateSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ServerStateSingleton, cls).__new__(cls)
            cls._instance.state = {}

            try:
                with open('store/server_state.pkl', 'rb') as f:
                    cls._instance.state = pickle.load(f)
            except FileNotFoundError:
                pass

        return cls._instance
    
    def save_state(self):
        with open('store/server_state.pkl', 'wb') as f:
            pickle.dump(self.state, f)
    
    def save_to_state(self, key, value):
        self.state[key] = value
        self.save_state()