import grpc
import node_pb2, node_pb2_grpc
import client_pb2, client_pb2_grpc
from role import Role, LeaseContext
from math import ceil
from random import randint
from election_timer import ElectionTimer
from address import NODE_IP_PORT, GRPC_DEADLINE
from node_servicer import NodeServicer
from client_servicer import ClientServicer
import sys, signal, os
from concurrent import futures

# TODO - integrate with GCP.

class Node:
    def __init__(self,id,ip_port) -> None:
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_len = 0
        self.current_role = Role.FOLLOWER    
        self.current_leader = None
        self.votes_recv = set()
        self.sent_len = {} #
        self.ack_len = {} #
        self.id = id
        self.nodes = []
        self.channels = {}
        self.stubs = {}
        self.database = {}
        self.election_timer = None
        self.ip_port = ip_port
        self.last_timer = -1
        self.lease_type = LeaseContext.SECONDARY #Someone else's lease
        self.lease_timer = ElectionTimer(self.last_timer,self.lease,False)
        self.not_enough_acks = False

        self.introduce_nodes()
        if not os.path.exists(f'logs_node_{self.id}'):
            os.mkdir(f'logs_node_{self.id}')
        else:
            self.recover_from_crash()
        self.start_server()          
    
    def handle_termination(self):
        if self.election_timer is not None:
            self.election_timer.cancel()
        if self.lease_timer is not None:
            self.lease_timer.cancel()
        print('\nClosing Server...')
        self.server.stop(0)
    
    def start_server(self):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        node_pb2_grpc.add_NodeServicer_to_server(NodeServicer(self),self.server)
        client_pb2_grpc.add_ClientServicer_to_server(ClientServicer(self),self.server)

        signal.signal(signal.SIGINT, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGTERM, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGALRM, lambda signum, frame : self.heartbeat())
        
        self.ip_port = "[::]:"+self.ip_port.split(":")[-1]
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

        with open(f'logs_node_{self.id}/logs.txt', 'r') as f:
            for line in f.readlines():
                line = line.split()
                if (line[0] == 'SET'):
                    self.database[line[1]] = line[2]
                msg = " ".join(line[:-1])
                self.log.append(node_pb2.LogEntry(term=int(line[-1]),msg=msg))

        with open(f'logs_node_{self.id}/metadata.txt', 'r') as f:
            # commit_length, current_term, voted_for
            line = f.readline().split()
            self.commit_len = int(line[0])
            self.current_term = int(line[1])
            self.voted_for = line[2]
    
    def make_grpc_call(self,method,request,node_id):
        self.channels[node_id] = grpc.insecure_channel(NODE_IP_PORT[node_id])
        self.stubs[node_id] = node_pb2_grpc.NodeStub(self.channels[node_id])
        response = None
        try:
            response = method(request,timeout=GRPC_DEADLINE)
        except grpc.RpcError as e:
            with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
                f.write(f"Error occurred while sending RPC to Node {node_id}.\n")
            print(e.code(),':',node_id,'is down')
            response = None
        return response
    
    def request_vote(self,node_id,term,candidate_id,last_log_index,last_log_term):
        if len(self.votes_recv) >= ceil((len(self.nodes) + 1) / 2):
            return
        print('Requesting vote from',node_id)
        response = self.make_grpc_call(self.stubs[node_id].RequestVote,node_pb2.RequestVoteRequest(term=term,candidate_id=candidate_id,
                                            last_log_index=last_log_index,last_log_term=last_log_term),node_id)

        if response is None:
            return

        if self.current_role == Role.CANDIDATE and self.current_term == response.term and response.vote_granted:
            self.votes_recv.add(node_id)
            print(self.votes_recv)
            if len(self.votes_recv) >= ceil((len(self.nodes) + 1) / 2):
                print(self.id," is the leader",self.current_term)
                self.lease_type = LeaseContext.SECONDARY
                self.current_role = Role.LEADER
                self.current_leader = self.id
                signal.setitimer(signal.ITIMER_REAL,1.5,1.5)
                    
                if self.election_timer:
                    print('Stopping Election Timer')
                    self.election_timer.cancel()
                
                if self.lease_timer:
                    self.lease_timer.cancel()

                self.log.append(node_pb2.LogEntry(term=self.current_term,msg="NO-OP"))
                # with open(f'logs_node_{self.id}/logs.txt', 'a') as f:
                    # f.write(f"NO-OP {self.current_term}\n")

                for node_id in self.nodes:
                    self.sent_len[node_id] = len(self.log)
                    self.ack_len[node_id] = 0
                    self.replicate_log(node_id)

                with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
                    f.write(f"Node {self.id} became the leader for term {self.current_term}.\n")

        elif response.term > self.current_term:
            self.current_term = response.term
            print('Voter has higher term')
            self.step_down()
        
        self.last_timer = max(self.last_timer, response.old_lease_timer)

    def on_broadcast_request(self,msg,log):
        if self.current_role != Role.LEADER:
            return self.current_leader, False, f"I am not the leader, {self.current_leader} is the leader"

        if self.current_role == Role.LEADER and self.lease_timer.get_timer() <= 0:
            return "", False, f"I am a leader but my lease is not renewed. {self.lease_timer.get_timer()}"
        
        if self.current_role == Role.LEADER and  self.lease_timer.get_timer() > 0:
            with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
                f.write(f"Node {self.id} (leader) received an {msg} request.\n")
            key = msg.split(' ')[1]
            if msg.startswith('SET') and self.lease_type == LeaseContext.PRIMARY:
                self.log.append(node_pb2.LogEntry(term=self.current_term,msg=msg))
                self.ack_len[self.id] = len(self.log)
                self.heartbeat()
                value = msg.split(' ')[2]
                self.database[key] = value
            elif msg.startswith('SET') and self.lease_type != LeaseContext.PRIMARY:
                return self.id, False, f"I am a leader with a valid lease, but my lease is not the leader lease."
            data = self.database[key] if key in self.database else ""
            return self.id, True, data

    def heartbeat(self):
        #Only leader can send heartbeats
        if self.current_role != Role.LEADER:
            print("ERROR! NON-LEADER SENDING HEARTBEATS!")
            return
        
        with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
            f.write(f"Leader {self.id} sending heartbeat & Renewing Lease\n")

        acks = 1
        if self.lease_type == LeaseContext.PRIMARY and not self.not_enough_acks:
            self.lease_timer.start(10,True)
        for node_id in self.nodes:
           acks += self.replicate_log(node_id)
        if self.lease_type == LeaseContext.PRIMARY:
            if acks >= len(self.nodes) // 2 + 1:
                # Balle Balle - You are the leader!
                self.not_enough_acks = False
            else:
                #Become a follower
                print('not enough acks')
                self.not_enough_acks = True

    def step_down(self):
        self.not_enough_acks = False
        print('Stepping down...')
        with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
            f.write(f"{self.id} Stepping down\n")
        signal.setitimer(signal.ITIMER_REAL,0,0)
        self.voted_for = None
        if self.election_timer:
            self.election_timer.reset()
        self.current_role = Role.FOLLOWER
        if self.lease_timer:
            self.lease_timer.cancel()

    def lease(self):
        if self.current_role == Role.LEADER and self.lease_type == LeaseContext.SECONDARY:
            # You have successfully waited for all other nodes' follower lease timers to run off.
            # You may now successfully become the leader!
            self.lease_timer.start(10)
            self.lease_type = LeaseContext.PRIMARY

        elif self.lease_type == LeaseContext.PRIMARY: 
            # You cannot establish comms with all other nodes. 
            # How do you have the leader lease, if you ain't the leader?!
            self.lease_type = LeaseContext.SECONDARY

            if self.current_role == Role.LEADER:
                print('Leader lost lease')
                with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
                    f.write(f"Leader {self.id} lease renewal failed. Stepping Down.\n")
                self.step_down()
        # print(f"{self.id} has the {self.lease_type} lease. It has timer remaining - {self.lease_timer.get_timer()}")
        
    def commit_log(self):
        acks = []
        min_acks = len(self.nodes) // 2 + 1
        ready = []
        #Optimization: Store the ack_len in sorted format and use one pointer in the second loop
        for node_id in self.nodes:
            if self.ack_len[node_id] >= 1:
                acks.append(node_id)

        for i in range(0,len(self.log)):
            if len(acks) + 1  >= min_acks:
                ready.append(i)
            for node_id in acks:
                if self.ack_len[node_id] < (i+1):
                    acks.remove(node_id)

        ready_max = max(ready) if len(ready) > 0 else 0
        if len(ready) != 0 and ready_max >= self.commit_len:
            with open(f'logs_node_{self.id}/logs.txt', 'a') as f:
                f2 = open(f'logs_node_{self.id}/dump.txt', 'a')
                for i in range(self.commit_len ,ready_max + 1):
                    f.write(f'{self.log[i].msg} {self.log[i].term}\n')
                    f2.write(f"Node {self.id} (leader) committed the entry {self.log[i].msg} to the state machine.\n")
                f2.close()
            
            self.commit_len = ready_max + 1
        with open(f'logs_node_{self.id}/metadata.txt', 'w') as f:
            f.write(f'{self.commit_len} {self.current_term} {self.voted_for}')

    def replicate_log(self,follower_id):
        prefix_len = self.sent_len[follower_id]
        # with open(f"test_{self.id}.txt", 'a') as f:
        #     f.write(f"pref: {prefix_len}, log: {self.log}, follower_ID: {follower_id}\n") 
        suffix = self.log[prefix_len:]
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len-1].term
        
        response = self.make_grpc_call(self.stubs[follower_id].LogRequest,node_pb2.LogRequestRequest(
            leader_id=self.id,term=self.current_term,
            prefix_len=prefix_len,prefix_term=prefix_term,
            leader_commit=self.commit_len,suffix=suffix, 
            lease_timer=self.lease_timer.get_timer()),follower_id)

        if response is None:
            return False

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

        return response.success

    def on_election(self):
        self.last_timer = -1
        with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
            f.write(f"Node {self.id} election timer timed out, Starting election.\n")
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
            self.request_vote(node_id,self.current_term,self.id,len(self.log),last_term)
        self.votes_recv = set()
        if self.current_role != Role.LEADER:
            print('Election Timer at election start')
            self.election_timer.start(randint(5,10))
        if self.current_role == Role.LEADER:
            self.last_timer = max(0,self.last_timer)
            self.lease_timer.start(self.last_timer)
            self.lease_type = LeaseContext.SECONDARY

            with open(f'logs_node_{self.id}/dump.txt', 'a') as f:
                f.write("New Leader waiting for Old Leader Lease to timeout.\n")
    
def main():
    if len(sys.argv) != 2:
        print("Usage: python node.py <node_id>")
        sys.exit(1)


    node_id = sys.argv[1]
    node = Node(node_id,NODE_IP_PORT[node_id])
    

if __name__ == "__main__":
    main()