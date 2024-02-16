import zmq
import threading
from datetime import datetime

message_server_ip = "34.131.87.117"
group_server_ip = "34.131.40.10"

# message_server_ip = "localhost"
# group_server_ip = "localhost"

context = zmq.Context()

message_server_socket = context.socket(zmq.REQ)
message_server_socket.connect(f"tcp://{message_server_ip}:5555")

user_socket = context.socket(zmq.REP)

def bind_socket_to_random_port(socket, min_port=5557, max_port=5600):
    for port in range(min_port, max_port + 1):
        try:
            socket.bind(f"tcp://*:{port}")
            return port
        except:
            continue
    return

port_number = bind_socket_to_random_port(user_socket)
usertele = set()
messages = [] #user, time (iso), message
message_lock = threading.Lock() 

def register_group():
    server_name = input("Enter group server name: ")
    message_server_socket.send_json(
        {
            "action":"JOIN",
            "server_name": server_name,
            "ip_address":group_server_ip,
            "port":f"{port_number}"
        }
    )
    status = message_server_socket.recv_json()['status']
    print(status)
    return status

def handle_join_request(request):
    global usertele
    print(f"JOIN REQUEST FROM {request['uuid']}")
    if request['uuid'] in usertele:
        print("ERROR! User already in the group.")
        user_socket.send_json({'status':'FAIL'})
        return
    usertele.add(request['uuid'])
    user_socket.send_json({'status':'SUCCESS'})
    return
    
def handle_leave_request(request):
    global usertele
    print(f"LEAVE REQUEST FROM {request['uuid']}")
    if request['uuid'] not in usertele:
        print("ERROR! User already not in the group.")
        user_socket.send_json({'status':'FAIL'})
        return
    usertele.remove(request['uuid'])
    user_socket.send_json({'status':'SUCCESS'})
    return

def handle_message_request(request):
    global messages
    global usertele
    print(f"MESSAGE REQUEST FROM {request['uuid']}")
    if request['uuid'] not in usertele:
        print("ERROR! User not in the group.")
        user_socket.send_json({'status':'FAIL'})
        return

    messages_to_send = []
    if request['is_date_time']:
        for message in messages:
            if datetime.fromisoformat(message[1]) >= datetime.fromisoformat(request['date_time']):
                messages_to_send.append(message)
    else:
        messages_to_send = messages
    user_socket.send_json({'messages':messages_to_send, 'status':'SUCCESS'})
    return

def handle_message_send(request):
    global messages
    global usertele
    print(f"MESSAGE SEND FROM {request['uuid']}")
    if request['uuid'] not in usertele:
        print("ERROR! User not in the group.")
        user_socket.send_json({'status':'FAIL'})
        return

    with message_lock:
        messages.append([request['uuid'], request['time'], request['message']])
    user_socket.send_json({'status':'SUCCESS'})
    return

def handle_request(request):
    if request['action'] == "JOIN":
        handle_join_request(request)
    elif request['action'] == "LEAVE":
        handle_leave_request(request)
    elif request['action'] == "GET":
        handle_message_request(request)
    elif request['action'] == "SEND":
        handle_message_send(request)
    else:
        print("ERROR! Wrong request action. Try again.")
        user_socket.send_json({'status': "FAIL"})
    return

def handle_requests():
    while True:
        try:
            if user_socket.poll(timeout=100):
                print("\n")
                request = user_socket.recv_json()
                threading.Thread(target=handle_request, args=(request,)).start()
        except KeyboardInterrupt:
            print("Exiting...")
            break


def main():
    action = input("Do you want to register? [y/n] ").upper()
    if action == "N":
        return
    elif action != 'Y':
        print("ERROR! Wrong input!")
        return

    if register_group() == "FAIL":
        return

    handle_requests()

        
if __name__ == "__main__":
    main()
