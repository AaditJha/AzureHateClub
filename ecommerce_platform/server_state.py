import json

class ServerStateSingleton:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ServerStateSingleton, cls).__new__(cls)
            cls._instance.state = {}

            try:
                with open('store/server_state.json', 'r') as f:
                    cls._instance.state = json.loads(f.read())
            except FileNotFoundError:
                pass

        return cls._instance
    
    def save_state(self):
        #Use json
        with open('store/server_state.json', 'w') as f:
            state_json = json.dumps(self.state, indent=4)
            f.write(state_json)
        

    def save_to_state(self, key, value):
        self.state[key] = value
        self.save_state()