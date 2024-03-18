import grpc
import node_pb2_grpc
import node_pb2
from node import Node
from role import Role

class NodeServicer(node_pb2_grpc.NodeServicer):
    def __init__(self, node : Node) -> None:
        super().__init__()
        self.node = node
    
    def RequestVote(self, request, context):
        c_term = request.term
        c_log_len = request.last_log_index
        c_id = request.candidate_id
        if c_term > self.node.current_term:
            self.node.current_term = c_term
            self.node.current_role = Role.FOLLOWER
        last_term = 0
        if len(self.node.log) > 0:
            last_term = self.node.log[-1].term
        log_ok = (c_term > last_term) or (c_term == last_term and c_log_len >= len(self.node.log))

        if c_term == self.node.current_term and log_ok and self.node.voted_for in [None,c_id]:
            self.node.voted_for = c_id
            return node_pb2.RequestVoteResponse(term=self.node.current_term,voter_id=self.node.id,vote_granted=True)
        else:
            return node_pb2.RequestVoteResponse(term=self.node.current_term,voter_id=self.node.id,vote_granted=False)