import grpc
import node_pb2
import node_pb2_grpc
from role import Role
from math import ceil

class Node:
    def __init__(self,id) -> None:
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_len = 0
        self.current_role = Role.FOLLOWER    
        self.current_leader = None
        self.votes_recv = set()
        self.sent_len = {}
        self.ack_len = {}
        self.id = id
        self.nodes = []
        self.channels = {}
        self.stubs = {}
    
    def introduce_nodes(self,node_ip_port):
        self.nodes = node_ip_port.keys()
        self.nodes.remove(self.node_id)
        for node_id in self.nodes:
            self.channels[node_id] = grpc.insecure_channel(node_ip_port[node_id])
            self.stubs[node_id] = node_pb2_grpc.NodeStub(self.channels[node_id])
    
    def recover_from_crash(self):
        self.current_role = Role.FOLLOWER
        self.current_leader = None
        self.votes_recv = {}
        self.sent_len = {}
        self.ack_len = {}
    
    def request_vote(self,node_id,term,candidate_id,last_log_index,last_log_term):
        response = self.stubs[node_id].RequestVote(
            node_pb2.RequestVoteRequest(term=term,candidate_id=candidate_id,
                                        last_log_index=last_log_index,last_log_term=last_log_term))
        if self.current_role == Role.CANDIDATE and self.current_term == response.term and response.vote_granted:
            self.votes_recv.add(node_id)
            if len(self.votes_recv) > ceil((len(self.nodes) + 1) / 2):
                self.current_role = Role.LEADER
                self.current_leader = self.id
                #cancel election timer
                for node_id in self.nodes:
                    self.sent_len[node_id] = len(self.log)
                    self.ack_len[node_id] = 0
                    #ReplicateLog on followers

    def on_election(self):
        self.current_term += 1
        self.current_role = Role.CANDIDATE
        self.voted_for = self.id 
        self.votes_recv.add(self.id)
        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        for node_id in self.nodes:
            self.request_vote(self,node_id,self.current_term,self.id,len(self.log),last_term)
        #start election timer
        