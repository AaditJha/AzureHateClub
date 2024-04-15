import grpc
from concurrent import futures
import os
import utils
import argparse
from address import MASTER_IP_PORT, MAPPER_IP_PORT, REDUCER_IP_PORT

# Create read entire data and create shards for mappers to read.
def create_shards(n_mappers:int, data_dir:str) -> list[list[int]]:
    shards = []
    with open(data_dir, 'r') as f:
        lines = f.readlines()
    lines_per_mapper = len(lines) // n_mappers
    start = 0
    for i in range(n_mappers):
        end = start + lines_per_mapper
        ids = [j for j in range(start, end)]
        shards.append(ids)
        start = end

    for i in range(end, len(lines)):
        shards[i % n_mappers].append(i)
    return shards


def spawn_mapper(shard:list[int]) -> None:
    pass

def run_master() -> None:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    shards = create_shards(args.M, args.data_dir) #shards[i] has the points for ith mapper.
    centroids = utils.gen_centroids(dim=len(shards[0])) #inital centroids.

    # Spawn mappers and make an RPC call to them.
    # Data to provide them is the shard and the centroids.


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Master program for running the MapReduce K-Means algorithm.")
    parser.add_argument("--M", type=int, help="Number of mappers")
    parser.add_argument("--R", type=int, help="Number of reducers")
    parser.add_argument("--K", type=int, help="Number of centroids")
    parser.add_argument("--max_iter", type=int, help="Number of iterations for K-Means")
    parser.add_argument("--data_dir", type=str, help="Path to data points")
    args = parser.parse_args()
    
    for i in range(args.M):
        if not os.path.exists(f"Data/Mappers/M{i + 1}"):
            os.makedirs(f"Data/Mappers/M{i + 1}")
    for i in range(args.R):
        if not os.path.exists(f"Data/Reducers/R{i + 1}"):
            print("YES")
            os.makedirs(f"Data/Reducers/R{i + 1}")


    run_master()

