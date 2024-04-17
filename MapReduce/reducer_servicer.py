import grpc
import reducer_pb2, reducer_pb2_grpc, mapper_pb2_grpc, mapper_pb2
from address import MAPPER_IP_PORT
from common import REDUCER_DIR
import numpy as np
import random

class ReducerServicer(reducer_pb2_grpc.ReducerServicer):
    def __init__(self, failure_prob) -> None:
        self.failure_prob = failure_prob

    def sort_by_key(self, keys, values):
        sorted_pairs = sorted(zip(keys, values), key=lambda pair: pair[0])
        sorted_k, sorted_v = zip(*sorted_pairs)
        return list(sorted_k), list(sorted_v)

    def ReduceRoutine(self, centroid_ids, points):
        sorted_centroid_ids, sorted_points = self.sort_by_key(centroid_ids, points)
        updated_centroid_ids, first_occ = np.unique(sorted_centroid_ids, return_index=True)
        updated_centroids = []
        first_occ = np.append(first_occ, len(sorted_points))
        for i in range(len(first_occ)-1):
            start = first_occ[i]
            end = first_occ[i+1]
            points = np.array(sorted_points[start:end])
            updated_centroids.append(np.mean(points, axis=0))
        
        return list(updated_centroid_ids), updated_centroids
    
    def CreateLocalFiles(self,reducer_id,centroid_ids, updated_centroids) -> None:
        with open(f"{REDUCER_DIR}/R{reducer_id}.txt", 'w') as f:
            for centroid_id, updated_centroid in zip(centroid_ids, updated_centroids):
                f.write(f"{centroid_id}, {updated_centroid}\n")

    def Reduce(self, request, context):
        if random.random() < self.failure_prob:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(f'Reducer {request.reducer_id} currently unavailable')
            return reducer_pb2.ReduceResponse()
        
        num_mappers = request.num_mappers
        for mapper_id in range(1, num_mappers+1):
            with grpc.insecure_channel(MAPPER_IP_PORT[mapper_id]) as channel:
                stub = mapper_pb2_grpc.MapperStub(channel)
                try:
                    response = stub.GetPairs(mapper_pb2.GetPairsRequest(reducer_id=request.reducer_id))
                except grpc.RpcError as e:
                    print('[ERROR]',e.details())
                    context.set_code(grpc.StatusCode.UNAVAILABLE)
                    context.set_details(f'Reducer {request.reducer_id} currently unavailable')
                    return reducer_pb2.ReduceResponse()
        
        centroid_ids, updated_centroids = self.ReduceRoutine(response.keys, [point.dim_val for point in response.values])

        reduce_response = reducer_pb2.ReduceResponse()
        for centroid_id, centroid in zip(centroid_ids, updated_centroids):
            centroid_point = reduce_response.updated_centroids.add()
            centroid_point.dim_val.extend(centroid)
            reduce_response.centroid_ids.append(centroid_id)

        self.CreateLocalFiles(request.reducer_id,centroid_ids, updated_centroids)        
    
        return reduce_response
    