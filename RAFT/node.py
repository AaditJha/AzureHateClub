import grpc
import node_pb2, node_pb2_grpc
import client_pb2, client_pb2_grpc
from role import Role
from math import ceil
from random import randint
from election_timer import ElectionTimer
from address import NODE_IP_PORT, GRPC_DEADLINE
from node_servicer import NodeServicer
from client_servicer import ClientServicer
import sys, signal, os
from concurrent import futures


class Node:
    def __init__(self,id,ip_port) -> None:
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
        self.database = {}
        self.election_timer = None
        self.ip_port = ip_port
        self.introduce_nodes()
        self.start_server()          
    
    def handle_termination(self):
        if self.election_timer is not None:
            self.election_timer.cancel()
        print('\nClosing Server...')
        self.server.stop(0)
    
    def start_server(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node_pb2_grpc.add_NodeServicer_to_server(NodeServicer(self),self.server)
        client_pb2_grpc.add_ClientServicer_to_server(ClientServicer(self),self.server)

        signal.signal(signal.SIGINT, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGTERM, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGALRM, lambda signum, frame : self.heartbeat())
        
        self.server.add_insecure_port(self.ip_port)
        self.server.start()
        print('Node',self.id,'started at',self.ip_port)

        self.election_timer = ElectionTimer(randint(5,10),self.on_election)
        print('Election Timer at server start')
        self.election_timer.start()

        # Register the signal handler
        self.server.wait_for_termination()

    def introduce_nodes(self):
        self.nodes = list(NODE_IP_PORT.keys())
        self.nodes.remove(self.id)
        for node_id in self.nodes:
            self.channels[node_id] = grpc.insecure_channel(NODE_IP_PORT[node_id])
            self.stubs[node_id] = node_pb2_grpc.NodeStub(self.channels[node_id])
            self.sent_len[node_id] = 0
            self.ack_len[node_id] = 0
    
    def recover_from_crash(self):
        '''
        This method reads the persistent states and updates the node accordingly.
        '''
        #TODO: Read Metadata, update state, Read Logs, update logs and database on the fly
        self.current_role = Role.FOLLOWER
        self.current_leader = None
        self.votes_recv = {}
        self.sent_len = {}
        self.ack_len = {}
        self.introduce_nodes()
    
    def make_grpc_call(self,method,request,node_id):
        response = None
        try:
            response = method(request,timeout=GRPC_DEADLINE)
        except grpc.RpcError as e:
            print(e.code(),':',node_id,'is down',e.details())
        return response
    
    def request_vote(self,node_id,term,candidate_id,last_log_index,last_log_term):
        response = self.make_grpc_call(self.stubs[node_id].RequestVote,node_pb2.RequestVoteRequest(term=term,candidate_id=candidate_id,
                                            last_log_index=last_log_index,last_log_term=last_log_term),node_id)

        if response is None:
            return

        if self.current_role == Role.CANDIDATE and self.current_term == response.term and response.vote_granted:
            self.votes_recv.add(node_id)
            print(self.votes_recv)
            if len(self.votes_recv) >= ceil((len(self.nodes) + 1) / 2):
                print(self.id," is the leader")
                self.current_role = Role.LEADER
                self.current_leader = self.id
                signal.setitimer(signal.ITIMER_REAL,1.5,1.5)
                if self.election_timer:
                    print('Stopping Election Timer')
                    self.election_timer.cancel()
                for node_id in self.nodes:
                    self.sent_len[node_id] = len(self.log)
                    self.ack_len[node_id] = 0
                    self.replicate_log(node_id)
    
        elif response.term > self.current_term:
            self.current_term = response.term
            self.current_role = Role.FOLLOWER
            signal.setitimer(signal.ITIMER_REAL,0,0)
            self.voted_for = None
            if self.election_timer:
                self.election_timer.reset()
        
    def on_broadcast_request(self,msg,log):
        if self.current_role != Role.LEADER:
            return self.current_leader, False, f"I am not the leader, {self.current_leader} is the leader"
        
        if self.current_role == Role.LEADER:
            key = msg.split(' ')[1]
            if msg.startswith('SET'):
                self.log.append(node_pb2.LogEntry(term=self.current_term,msg=msg))
                self.ack_len[self.id] = len(self.log)
                self.heartbeat()
                value = msg.split(' ')[2]
                self.database[key] = value
            data = self.database[key] if key in self.database else ""
            return self.id, True, data

    def heartbeat(self):
        for node_id in self.nodes:
            self.replicate_log(node_id)
        
    def commit_log(self):
        print("HEY LEADER")
        acks = []
        min_acks = ceil((len(self.nodes) + 1) / 2)
        ready = []
        #Optimization: Store the ack_len in sorted format and use one pointer in the second loop
        for node_id in self.nodes:
            if self.ack_len[node_id] >= 1:
                acks.append(node_id)

        for i in range(1,len(self.log)):
            if len(acks) >= min_acks:
                ready.append(i)
            for node_id in acks:
                if self.ack_len[node_id] < (i+1):
                    acks.remove(node_id)
        ready_max = max(ready) if len(ready) > 0 else 0
        if len(ready) != 0 and ready_max > self.commit_len and self.log[ready_max - 1].term == self.current_term:
            for i in range(self.commit_len,ready_max):
                #TODO: deliver log[i] msg to application
                print("HEY LEADER LOOP")
            self.commit_len = ready_max

    def replicate_log(self,follower_id):
        prefix_len = self.sent_len[follower_id]
        suffix = self.log[prefix_len:]
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len-1].term
        
        response = self.make_grpc_call(self.stubs[follower_id].LogRequest,node_pb2.LogRequestRequest(leader_id=self.id,term=self.current_term,
                                        prefix_len=prefix_len,prefix_term=prefix_term,
                                        leader_commit=self.commit_len,suffix=suffix),follower_id)

        if response is None:
            return

        if response.term == self.current_term and self.current_role == Role.LEADER:
            if response.success and response.ack >= self.ack_len[follower_id]:
                self.sent_len[follower_id] = response.ack
                self.ack_len[follower_id] = response.ack
                self.commit_log()
            elif self.sent_len[follower_id] > 0:
                self.sent_len[follower_id] -= 1
                self.replicate_log(follower_id)
        elif response.term > self.current_term:
            self.current_term = response.term
            self.current_role = Role.FOLLOWER
            signal.setitimer(signal.ITIMER_REAL,0,0)
            self.voted_for = None
            if self.election_timer:
                self.election_timer.reset()

    def on_election(self):
        print(self.id," starting election")
        self.current_term += 1
        self.current_role = Role.CANDIDATE
        signal.setitimer(signal.ITIMER_REAL,0,0)
        self.voted_for = self.id 
        self.votes_recv = set()
        self.votes_recv.add(self.id)
        print(self.votes_recv)
        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        for node_id in self.nodes:
            print('Requesting vote from',node_id)
            self.request_vote(node_id,self.current_term,self.id,len(self.log),last_term)
        self.votes_recv = set()
        if self.current_role != Role.LEADER:
            print('Election Timer at election start')
            self.election_timer.start(randint(5,10))        
    
def main():
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)


    node_id = sys.argv[1]
    node = Node(node_id,NODE_IP_PORT[node_id])
    if not os.path.exists(f'logs_node_{node_id}'):
        os.mkdir(f'logs_node_{node_id}')
    # else:
    #     node.recover_from_crash()

if __name__ == "__main__":
    main()