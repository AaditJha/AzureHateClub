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
import numpy as np

def mapper_call(shard:list[int], centroids:list[list[float]], mapper_id:int) -> None:

    with grpc.insecure_channel(MAPPER_IP_PORT[mapper_id]) as channel:
        stub = mapper_pb2_grpc.MapperStub(channel)
        request = utils.create_map_request(shard=shard, centroids=centroids, num_reducers=args.R)
        response = stub.Map(request)

    if not response.success:
        raise Exception(f"Mapper {mapper_id} currently unavailable")

def spawn_mapper(shard:list[int], centroids:list[list[float]], mapper_id:int) -> None:
    while True:
        try:
            mapper_call(shard, centroids, mapper_id)
            print(f"Mapper {mapper_id} successfully mapped.")
            return
        except Exception as e:
            print('[ERROR]',e)
            print(f"Mapper {mapper_id} failed.")
            time.sleep(2)

centroid_dict = {}

def spawn_reducer(reducer_id:int) -> None:
    while True:
        try:
            with grpc.insecure_channel(REDUCER_IP_PORT[reducer_id]) as channel:
                stub = reducer_pb2_grpc.ReducerStub(channel)
                response = stub.Reduce(reducer_pb2.ReduceRequest(reducer_id=reducer_id,num_mappers=args.M))
                for centroid_id, centroid in zip(response.centroid_ids, response.updated_centroids):
                    centroid_dict[centroid_id] = centroid.dim_val
                print(f"Reducer {reducer_id} successfully reduced.")
                return
            
        except grpc.RpcError as e:
            print("IAFDHUIFHADUI")
            print('[ERROR]',e.details())
            print(f"Reducer {reducer_id} failed.")
            time.sleep(5)


def run_master(iter) -> None:
    shards, dim = utils.create_shards(args.M) #shards[i] has the points for ith mapper.
    centroids = utils.gen_centroids(dim=dim, n_centroids=args.K, n_points=sum([len(_) for _ in shards])) #inital centroids.

    for i in range(iter):
        global centroid_dict
        print('Running iteration:', i+1)
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
        
        # Spawn reducers and make an RPC call to them.
        reducer_threads = []
        for i in range(args.R):
            thread = threading.Thread(target=spawn_reducer, args=(i+1,))
            reducer_threads.append(thread)
        
        for thread in reducer_threads:
            thread.start()
        
        for thread in reducer_threads:
            thread.join()
        
        converged = True
        # Update centroids.
        for i in range(args.K):
            if np.linalg.norm(np.array(list(centroid_dict[i])) - np.array(centroids[i])) > 1e-6:
                converged = False
            centroids[i] = list(centroid_dict[i])
        
        centroid_dict = {}
        if converged:
            print('Converged.')
            break

        print('-'*50)
    
    with open("Data/centroids.txt", 'w') as f:
        for centroid in centroids:
            for dim in centroid[:-1]:
                f.write(f"{dim}, ")
            f.write(f"{centroid[-1]}\n")


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

    run_master(args.max_iter)

