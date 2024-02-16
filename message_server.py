import zmq

context = zmq.Context()

group_server_socket = context.socket(zmq.REP)
group_server_socket.bind("tcp://*:5555")

user_socket = context.socket(zmq.REP)
user_socket.bind("tcp://*:5556")

group_servers = {}

def handle_group_registration():
    request = group_server_socket.recv_json()
    if request["action"] == "JOIN":
        server_name = request["server_name"]
        ip_address = request["ip_address"]
        port = request["port"]
        if server_name in group_servers:
            print("ERROR! Server already registered.")
            group_server_socket.send_json({"status": "FAIL"})
            return
        group_servers[server_name] = (ip_address, port)
        print(f"JOIN REQUEST FROM {request['ip_address']}:{request['port']}")
        group_server_socket.send_json({"status": "SUCCESS"})
        return
    else:
        print("ERROR! Request is not JOIN.")
        group_server_socket.send_json({"status": "FAIL"})
        return

def handle_user_requests():
    request = user_socket.recv_json()
    if request["action"] == "GROUP_LIST":
        print(f"GROUP LIST REQUEST FROM {request['uuid']}")
        user_socket.send_json({"groups": group_servers, "status": "SUCCESS"})
        return
    else:
        print("ERROR! Request is not GROUP_LIST.")
        user_socket.send_json({"status": "FAIL"})
        return

def main():
    while True:
        if group_server_socket.poll(timeout=100):
            print("\n")
            handle_group_registration()
        if user_socket.poll(timeout=100):
            print("\n")
            handle_user_requests()
        
if __name__ == "__main__":
    main()
