import grpc
import mapper_pb2, mapper_pb2_grpc, mapper_pb2
import utils
import random
import os
from common import NUM_REDUCERS, MAPPER_DIR

class MapperServicer(mapper_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id:int, failure_prob:float) -> None:
        self.mapper_id = mapper_id
        self.failure_prob = failure_prob
        if os.path.exists(f"{MAPPER_DIR}/M{self.mapper_id}"):
            print('Reading from local files')
            partition_files = sorted([f for f in os.listdir(f"{MAPPER_DIR}/M{self.mapper_id}") if f.endswith('.txt')])
            self.partitions = {k : [] for k in range(len(partition_files))}
            for f in partition_files:
                partition_id = int(f.split('_')[-1].split('.')[0])
                with open(f"{MAPPER_DIR}/M{self.mapper_id}/{f}", 'r') as f:
                    lines = f.readlines()
                    self.partitions[partition_id] = [(int(line.split(',')[0]), list(map(float , line.split(',')[1:-1]))) for line in lines]

    def Map(self, request:mapper_pb2.MapRequest, context:str) -> mapper_pb2.MapResponse:
        if random.random() < self.failure_prob:
            return mapper_pb2.MapResponse(success=False)
        
        self.point_ids, self.centroids = utils.read_map_request(map_request=request)
        self.MapRoutine()
        self.Partition(request.num_reducers)
        self.CreateLocalFiles(request.write_mode)
        return mapper_pb2.MapResponse(success=True)

    def MapRoutine(self) -> None:
        self.points = [utils.get_point_from_id(idx) for idx in self.point_ids]
        self.closest_centroid = [utils.get_closest_centroid(point, self.centroids) for point in self.points]
    
    # creates a list of tuple[int, list[float]]
    def Partition(self,num_reducers) -> None:
        self.partitions = {k:[] for k in range(num_reducers)}
        for i in range(len(self.closest_centroid)):
            reducer_id = self.closest_centroid[i] % num_reducers
            self.partitions[reducer_id].append((self.closest_centroid[i], self.points[i]))


    def CreateLocalFiles(self, write_mode) -> None:
        for partition_id, partition in self.partitions.items():
            with open(f"{MAPPER_DIR}/M{self.mapper_id}/partition_{partition_id+1}.txt", write_mode) as f:
                for centroid, point in partition:
                    f.write(f"{centroid}, ")
                    for p in point:
                        f.write(f"{p}, ")
                    f.write('\n')
    

    def GetPairs(self, request, context):
        reducer_id = request.reducer_id - 1
        if len(self.partitions[reducer_id]) == 0:
            return mapper_pb2.GetPairsResponse(keys=[], values=[])
        keys, values = list(zip(*self.partitions[reducer_id]))
        points = [mapper_pb2.Point(dim_val=list(v)) for v in values]
        return mapper_pb2.GetPairsResponse(keys=list(keys), values=points)