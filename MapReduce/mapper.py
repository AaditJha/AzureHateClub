import grpc
from concurrent import futures
import utils
import argparse
from address import MASTER_IP_PORT, MAPPER_IP_PORT
import mapper_pb2, mapper_pb2_grpc
from mapper_servicer import MapperServicer

class Mapper:
    def __init__(self, mapper_id:int, failure_prob:float) -> None:
        self.mapper_id = mapper_id
        self.ip_port = MAPPER_IP_PORT[mapper_id]
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        mapper_pb2_grpc.add_MapperServicer_to_server(MapperServicer(mapper_id=self.mapper_id,failure_prob=failure_prob), self.server)
        self.server.add_insecure_port(self.ip_port)
        print(f"Starting Mapper with IP/PORT {self.ip_port}")
        self.server.start()  
        self.server.wait_for_termination()

    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Mapper program for running the MapReduce K-Means algorithm.")
    parser.add_argument("--id", type=int, help="ID of mapper, in range [1, ..., M]")
    parser.add_argument("--prob", default=0.5 , type=float, help="Failure Probability")
    # parser.add_argument("--data_dir", type=str, help="Data directory")
    args = parser.parse_args()
    Mapper(mapper_id=args.id,failure_prob=args.prob)