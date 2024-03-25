import grpc
import client_pb2
import client_pb2_grpc
from address import NODE_IP_PORT, GRPC_DEADLINE

import random

class Client:
    def __init__(self) -> None:
        self.current_leader = list(NODE_IP_PORT.keys())[0]
        self.channels = {}
        self.stubs = {}
        for node_id in NODE_IP_PORT:
            self.channels[node_id] = grpc.insecure_channel(NODE_IP_PORT[node_id])
            self.stubs[node_id] = client_pb2_grpc.ClientStub(self.channels[node_id])
    
    def broadcast_request(self,msg):
        response = self.make_grpc_call(self.stubs[self.current_leader].QueryServer, \
                                       client_pb2.QueryServerRequest(msg=msg), self.current_leader)
        if response is not None and response.leader_id != '':
            print(response.leader_id)
            return response.leader_id,response.success,response.data
        return list(NODE_IP_PORT.keys())[random.randint(0, len(NODE_IP_PORT.keys())-1)],False,''

    def make_grpc_call(self,method,request,node_id):
        response = None
        try:
            response = method(request,timeout=GRPC_DEADLINE)
        except grpc.RpcError as e:
            print(e.code(),':',node_id,'is down',e.details())
        return response

if __name__ == "__main__":
    client = Client()
    while True:
        op = input('Enter operation (1 for SET, 2 for GET, 3 for EXIT): ')
        if op == '1':
            K = input('Enter key: ')
            V = input('Enter value: ')
            msg = f'SET {K} {V}'
        elif op == '2':
            K = input('Enter key: ')
            msg = f'GET {K}'
        elif op == '3':
            break
        else:
            print('Invalid operation')
            continue
        
        client.current_leader,success,data = client.broadcast_request(msg)
        while not success:
            print('ERROR',data)
            client.current_leader,success,data = client.broadcast_request(msg)
        
        if op == '2':
            print(f'Value of {K}: {data}')