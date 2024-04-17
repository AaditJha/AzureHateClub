import grpc
from concurrent import futures
import utils
import argparse
from address import REDUCER_IP_PORT
import reducer_pb2, reducer_pb2_grpc
from reducer_servicer import ReducerServicer

class Reducer:
    def __init__(self, reducer_id, failure_prob) -> None:
        self.ip_port = REDUCER_IP_PORT[reducer_id]
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        reducer_pb2_grpc.add_ReducerServicer_to_server(ReducerServicer(failure_prob), self.server)
        self.server.add_insecure_port(self.ip_port)
        print(f"Starting Reducer with IP/PORT {self.ip_port}")
        self.server.start()  
        self.server.wait_for_termination()

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Reducer program for running the MapReduce K-Means algorithm.")
    parser.add_argument("--id", type=int, help="ID of reducer, in range [1, ..., M]")
    parser.add_argument("--prob", default=0.5 , type=float, help="Failure Probability")
    # parser.add_argument("--data_dir", type=str, help="Data directory")
    args = parser.parse_args()
    Reducer(reducer_id=args.id, failure_prob=args.prob)