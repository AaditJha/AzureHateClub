import random
import linecache 
import numpy as np
import mapper_pb2, mapper_pb2_grpc
from common import DATA_DIR


def gen_centroids(dim:int, n_centroids:int) -> list[list[float]]:
    return [[(random.random() - 0.5) * 10 for _ in range(dim)] for __ in range(n_centroids)]

def euclidean_dist(point:list[float], centroid:list[float]) -> float:
    return ((np.array(point) -  np.array(centroid)) ** 2).sum() ** 0.5

def get_closest_centroid(point:list[float], centroids:list[list[float]]) -> int:
    return np.argmin(np.array([euclidean_dist(point, centroid) for centroid in centroids]))

# Create read entire data and create shards for mappers to read.
def create_shards(n_mappers:int) -> tuple[list[list[int]],int]:
    shards = []
    with open(DATA_DIR, 'r') as f:
        for lines, line in enumerate(f):
            pass
        lines += 1
    lines_per_mapper = lines // n_mappers
    start = 0
    for i in range(n_mappers):
        end = start + lines_per_mapper
        ids = [j for j in range(start, end)]
        shards.append(ids)
        start = end

    for i in range(end, lines):
        shards[i % n_mappers].append(i)
    return shards, len(get_point_from_id(idx=0))

def get_point_from_id(idx:int) -> list[float]:
    return [float(element.strip()) for element in linecache.getline(filename=DATA_DIR, lineno=idx+1).split(', ')]

def create_map_request(shard: list[int], centroids: list[list[float]], num_reducers: int) -> mapper_pb2.MapRequest:
    map_request = mapper_pb2.MapRequest()

    for centroid_list in centroids:
        centroid_point = map_request.centroids.add()
        centroid_point.dim_val.extend(centroid_list)

    map_request.point_ids.extend(shard)

    map_request.num_reducers = num_reducers
    return map_request

def read_map_request(map_request: mapper_pb2.MapRequest) -> tuple[list[int], list[list[float]]]:
    shard = []
    centroids = []
    for centroid in map_request.centroids:
        centroids.append([val for val in centroid.dim_val])
    for point_id in map_request.point_ids:
        shard.append(point_id)
    return shard, centroids

def read_map_response(map_response: mapper_pb2.MapResponse) -> list[tuple[int, list[float]]]:
    partition = []

    for key_value in map_response.pairs:
        centroid_id = key_value.closest_centroid
        point_values = list(key_value.point.dim_val)
        partition.append((centroid_id, point_values))

    return partition

