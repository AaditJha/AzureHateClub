import zmq
import uuid
from datetime import datetime, time

message_server_ip = "34.131.87.117"

# message_server_ip = "localhost"

context = zmq.Context()

message_server_socket = context.socket(zmq.REQ)
message_server_socket.connect(f"tcp://{message_server_ip}:5556")

groups = {}
user_id = str(uuid.uuid4())

def get_group_list():
    global groups
    message_server_socket.send_json(
        {
            "uuid":user_id,
            "action":"GROUP_LIST",
        }
    )
    response = message_server_socket.recv_json()
    print(response['status'])
    if response['status'] == 'SUCCESS':
        groups = response['groups']
        for s_name in groups.keys():
            print(f'{s_name} - {groups[s_name][0]}:{groups[s_name][1]}')
    return

def join_group():
    group_name = input("Enter the group name: ")
    try:
        group_server_socket = context.socket(zmq.REQ)
        group_server_socket.connect(f"tcp://{groups[group_name][0]}:{groups[group_name][1]}")
    except:
        print("ERROR! Wrong group name entered.")
        return
    group_server_socket.send_json(
        {
            "uuid":user_id,
            "action":"JOIN",
        }
    )
    response = group_server_socket.recv_json()
    print(response['status'])
    return

def leave_group():
    group_name = input("Enter the group name: ")
    try:
        group_server_socket = context.socket(zmq.REQ)
        group_server_socket.connect(f"tcp://{groups[group_name][0]}:{groups[group_name][1]}")
    except:
        print("ERROR! Wrong group name entered.")
        return
    group_server_socket.send_json(
        {
            "uuid":user_id,
            "action":"LEAVE",
        }
    )
    response = group_server_socket.recv_json()
    print(response['status'])
    return

def get_message():
    inp_str = input("Enter the group_name [SPACE/OPTIONAL] time_for_message (HH:MM:SS): ")
    inp_params = inp_str.split()
    group_name = inp_params[0]
    datetime_object = None
    try:
        if len(inp_params) > 1:
            time_string = inp_params[1]
            today_date = datetime.today().date()
            time_object = datetime.strptime(time_string, '%H:%M:%S').time()
            datetime_object = datetime.combine(today_date, time_object).isoformat()
        group_server_socket = context.socket(zmq.REQ)
        group_server_socket.connect(f"tcp://{groups[group_name][0]}:{groups[group_name][1]}")
    except:
        print("ERROR! Wrong group name/time entered.")
        return
    
    group_server_socket.send_json(
        {
            "uuid":user_id,
            "action":"GET",
            "is_date_time": len(inp_params) > 1,
            "date_time":datetime_object,
        }
    )

    response = group_server_socket.recv_json()
    print(response['status'])
    if response['status'] == "SUCCESS":
        for message in response['messages']:
            print(message)
    return

def send_message():
    group_name = input("Enter the group_name: ")
    message = input("Enter the message to send (1 line max): ")
    try:
        group_server_socket = context.socket(zmq.REQ)
        group_server_socket.connect(f"tcp://{groups[group_name][0]}:{groups[group_name][1]}")
    except:
        print("ERROR! Wrong group name entered.")
        return
    
    group_server_socket.send_json(
        {
            "uuid":user_id,
            "action":"SEND",
            "message": message,
            "time": datetime.now().isoformat()
        }
    )

    response = group_server_socket.recv_json()
    print(response['status'])
    return


def main():
    action = input("Do you want to register? [y/n] ").upper()
    if action == "N":
        return
    elif action != 'Y':
        print("ERROR! Wrong input!")
        return
    
    while True:
        action = input("\n\nWhat do you want to do? Please enter action code: ").upper()
        if action == "GROUP_LIST":
            get_group_list()
        elif action == "JOIN":
            join_group()
        elif action == "LEAVE":
            leave_group()
        elif action == "GET":
            get_message()
        elif action == "SEND":
            send_message()
        else:
            print("ERROR! Wrong input. Try again.")


if __name__ == "__main__":
    main()
