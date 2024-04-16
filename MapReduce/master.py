import grpc
import threading
from concurrent import futures
import os
import time
import utils
import argparse
from address import MASTER_IP_PORT, MAPPER_IP_PORT, REDUCER_IP_PORT
import common
import mapper_pb2, mapper_pb2_grpc
import reducer_pb2, reducer_pb2_grpc

def mapper_call(shard:list[int], centroids:list[list[float]], mapper_id:int) -> None:

    with grpc.insecure_channel(MAPPER_IP_PORT[mapper_id]) as channel:
        stub = mapper_pb2_grpc.MapperStub(channel)
        request = utils.create_map_request(shard=shard, centroids=centroids, num_reducers=args.R)
        response = stub.Map(request)

def spawn_mapper(shard:list[int], centroids:list[list[float]], mapper_id:int) -> None:
    while True:
        try:
            mapper_call(shard, centroids, mapper_id)
            print(f"Mapper {mapper_id} successfully mapped.")
            return
        except grpc.RpcError as e:
            print(e.details())
            print(f"Mapper {mapper_id} failed.")
            time.sleep(5)


def run_master() -> None:
    shards, dim = utils.create_shards(args.M) #shards[i] has the points for ith mapper.
    centroids = utils.gen_centroids(dim=dim, n_centroids=args.K) #inital centroids.
    
    # Spawn mappers and make an RPC call to them.
    # Data to provide them is the shard and the centroids.
    mapper_threads = []
    for i in range(args.M):
        thread = threading.Thread(target=spawn_mapper, args=(shards[i], centroids, i+1))
        mapper_threads.append(thread)

    for thread in mapper_threads:
        thread.start()
    
    for thread in mapper_threads:
        thread.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Master program for running the MapReduce K-Means algorithm.")
    parser.add_argument("--M", default=common.NUM_MAPPERS, type=int, help="Number of mappers")
    parser.add_argument("--R", default=common.NUM_REDUCERS, type=int, help="Number of reducers")
    parser.add_argument("--K", default=common.NUM_CENTROIDS, type=int, help="Number of centroids")
    parser.add_argument("--max_iter", default=5, type=int, help="Number of iterations for K-Means")
    parser.add_argument("--data_dir", default=common.DATA_DIR, type=str, help="Path to data points")
    args = parser.parse_args()
    
    for i in range(args.M):
        if not os.path.exists(f"Data/Mappers/M{i + 1}"):
            os.makedirs(f"Data/Mappers/M{i + 1}")
    for i in range(args.R):
        if not os.path.exists(f"Data/Reducers/R{i + 1}"):
            os.makedirs(f"Data/Reducers/R{i + 1}")


    run_master()

