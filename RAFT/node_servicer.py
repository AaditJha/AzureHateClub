import grpc
import node_pb2_grpc
import node_pb2
from role import Role
import signal

class NodeServicer(node_pb2_grpc.NodeServicer):
    def __init__(self, node) -> None:
        super().__init__()
        self.node = node
    
    def RequestVote(self, request, context):
        if self.node.current_role == Role.LEADER:
            return node_pb2.RequestVoteResponse(term=self.node.current_term,
                                                voter_id=self.node.id,
                                                vote_granted=False,
                                                old_lease_timer=self.node.lease_timer.get_timer())

        c_term = request.term
        c_log_len = request.last_log_index
        c_id = request.candidate_id
        if c_term > self.node.current_term:
            self.node.current_term = c_term
            self.node.current_role = Role.FOLLOWER
            self.node.voted_for = None
        last_term = 0
        if len(self.node.log) > 0:
            last_term = self.node.log[-1].term
        log_ok = (c_term > last_term) or (c_term == last_term and c_log_len >= len(self.node.log))

        print('Voted for:',self.node.voted_for,'Requested by:',c_id)
        timer = self.node.lease_timer.get_timer() if self.node.lease_timer else 0
        if c_term == self.node.current_term and log_ok and self.node.voted_for in [None,c_id]:
            self.node.voted_for = c_id
            print('granted vote to',c_id)
            with open(f'logs_node_{self.node.id}/dump.txt', 'a') as f:
                f.write(f"Vote granted for Node {c_id} in term {c_term}.\n")
            return node_pb2.RequestVoteResponse(term=self.node.current_term,
                                                voter_id=self.node.id,
                                                vote_granted=True,
                                                old_lease_timer=timer)
        else:
            with open(f'logs_node_{self.node.id}/dump.txt', 'a') as f:
                f.write(f"Vote denied for Node {c_id} in term {c_term}.\n")
            return node_pb2.RequestVoteResponse(term=self.node.current_term,
                                                voter_id=self.node.id,
                                                vote_granted=False,
                                                old_lease_timer=timer)
        
    def append_entries(self,prefix_len,leader_commit,suffix):
        if len(suffix) > 0 and len(self.node.log) > prefix_len:
            index = min(len(self.node.log),prefix_len+len(suffix)) - 1
            if self.node.log[index].term != suffix[index - prefix_len].term:
                self.node.log = self.node.log[:prefix_len]
        
        if prefix_len + len(suffix) > len(self.node.log):
            with open(f'logs_node_{self.node.id}/dump.txt', 'a') as f:
                f.write(f"Node {self.node.id} accepted AppendEntries RPC from {self.node.current_leader}.\n")
            for i in range(len(self.node.log)-prefix_len, len(suffix)):
                self.node.log.append(suffix[i])
                msg = suffix[i].msg
                if msg.startswith('SET'):
                    key,value = msg.split(' ')[1:]
                    self.node.database[key] = value

        if leader_commit > self.node.commit_len:
            with open(f'logs_node_{self.node.id}/logs.txt', 'a') as f:
                f2 = open(f'logs_node_{self.node.id}/dump.txt', 'a')
                for i in range(self.node.commit_len,leader_commit):
                    f.write(f'{self.node.log[i].msg} {self.node.log[i].term}\n')
                    f2.write(f"Node {self.node.id} (follower) committed the entry {self.node.log[i].msg} to the state machine.\n")
                f2.close()

            self.node.commit_len = leader_commit
            with open(f'logs_node_{self.node.id}/metadata.txt', 'w') as f:
                f.write(f'{self.node.commit_len} {self.node.current_term} {self.node.voted_for}')

    def LogRequest(self, request, context):
        if request.term > self.node.current_term:
            self.node.current_term = request.term
            self.node.voted_for = None
            if self.node.election_timer:
                self.node.election_timer.reset()
        
        if request.term == self.node.current_term:
            self.node.current_role = Role.FOLLOWER
            self.node.current_leader = request.leader_id
            if self.node.election_timer:
                self.node.election_timer.reset()
        

        #Does not matter is leader's lease_type is Role.LEADER or Role.FOLLOWER
        self.node.lease_timer.start(request.lease_timer,True)

        log_ok = (len(self.node.log) >= request.prefix_len) and (request.prefix_len == 0 or 
                                                                 self.node.log[request.prefix_len-1].term == request.prefix_term)


        if request.term == self.node.current_term and log_ok:
            self.append_entries(request.prefix_len,request.leader_commit,request.suffix)
            ack = request.prefix_len + len(request.suffix)
            return node_pb2.LogRequestResponse(follower_id=self.node.id,term=self.node.current_term,ack=ack,success=True)

        with open(f'logs_node_{self.node.id}/dump.txt', 'a') as f:
            f.write(f"Node {self.node.id} rejected AppendEntries RPC from {request.leader_id}.\n")
        return node_pb2.LogRequestResponse(follower_id=self.node.id,term=self.node.current_term,ack=0,success=False)