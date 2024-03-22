import grpc
import client_pb2_grpc
import client_pb2
from role import Role

class ClientServicer(client_pb2_grpc.ClientServicer):
    def __init__(self, node) -> None:
        super().__init__()
        self.node = node

    def QueryServer(self, request, context):
        log = False
        if request.msg.startswith('SET'):
            log = True
        current_leader,success,data = self.node.on_broadcast_request(msg=request.msg,log=log)
        return client_pb2.QueryServerResponse(leader_id=current_leader,success=success,data=data)
