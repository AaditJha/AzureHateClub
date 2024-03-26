import grpc
import node_pb2_grpc
import node_pb2
from role import Role

class NodeServicer(node_pb2_grpc.NodeServicer):
    def __init__(self, node) -> None:
        super().__init__()
        self.node = node
    
    def RequestVote(self, request, context):
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
        if c_term == self.node.current_term and log_ok and self.node.voted_for in [None,c_id]:
            self.node.voted_for = c_id
            print('granted vote to',c_id)
            return node_pb2.RequestVoteResponse(term=self.node.current_term,voter_id=self.node.id,vote_granted=True)
        else:
            return node_pb2.RequestVoteResponse(term=self.node.current_term,voter_id=self.node.id,vote_granted=False)
        
    def append_entries(self,prefix_len,leader_commit,suffix):
        if len(suffix) > 0 and len(self.node.log) > prefix_len:
            index = min(len(self.node.log),prefix_len+len(suffix)) - 1
            if self.node.log[index].term != suffix[index - prefix_len].term:
                self.node.log = self.node.log[:prefix_len]
        
        if prefix_len + len(suffix) > len(self.node.log):
            for i in range(len(self.node.log)-prefix_len, len(suffix)):
                self.node.log.append(suffix[i])
                msg = suffix[i].msg
                if msg.startswith('SET'):
                    key,value = msg.split(' ')[1:]
                    self.node.database[key] = value
        
        print("leader_commit:", leader_commit)
        print("commit_len:", self.node.commit_len)

        if leader_commit > self.node.commit_len:
            print("_________IN DA LOOP________")
            for i in range(self.node.commit_len,leader_commit):
                print(self.node.log[i])
                #TODO: Store the new logs for follower
            self.node.commit_len = leader_commit

    def LogRequest(self, request, context):
        if request.term > self.node.current_term:
            self.node.current_term = request.term
            self.node.voted_for = None
            if self.node.election_timer:
                print("Resetting election timer")
                self.node.election_timer.reset()
        
        if request.term == self.node.current_term:
            self.node.current_role = Role.FOLLOWER
            self.node.current_leader = request.leader_id
            if self.node.election_timer:
                print("Resetting election timer")
                self.node.election_timer.reset()

        log_ok = (len(self.node.log) >= request.prefix_len) and (request.prefix_len == 0 or 
                                                                 self.node.log[request.prefix_len-1].term == request.prefix_term)


        if request.term == self.node.current_term and log_ok:
            self.append_entries(request.prefix_len,request.leader_commit,request.suffix)
            ack = request.prefix_len + len(request.suffix)
            return node_pb2.LogRequestResponse(follower_id=self.node.id,term=self.node.current_term,ack=ack,success=True)
    
        return node_pb2.LogRequestResponse(follower_id=self.node.id,term=self.node.current_term,ack=0,success=False)