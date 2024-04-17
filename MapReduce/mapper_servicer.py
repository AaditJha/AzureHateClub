import grpc
import mapper_pb2, mapper_pb2_grpc, mapper_pb2
import utils
from common import NUM_REDUCERS, MAPPER_DIR

class MapperServicer(mapper_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id:int) -> None:
        self.mapper_id = mapper_id

    def Map(self, request:mapper_pb2.MapRequest, context:str) -> mapper_pb2.MapResponse:
        self.point_ids, self.centroids = utils.read_map_request(map_request=request)
        self.MapRoutine()
        self.Partition(request.num_reducers)
        self.CreateLocalFiles()
        return mapper_pb2.MapResponse(success=True)

    def MapRoutine(self) -> None:
        self.points = [utils.get_point_from_id(idx) for idx in self.point_ids]
        self.closest_centroid = [utils.get_closest_centroid(point, self.centroids) for point in self.points]
    
    # creates a list of tuple[int, list[float]]
    def Partition(self,num_reducers) -> None:
        self.partitions = [[] for _ in range(num_reducers)]
        for i in range(len(self.closest_centroid)):
            reducer_id = self.closest_centroid[i] % num_reducers
            self.partitions[reducer_id].append((self.closest_centroid[i], self.points[i]))


    def CreateLocalFiles(self) -> None:
        for partition_id, partition in enumerate(self.partitions):
            with open(f"{MAPPER_DIR}/M{self.mapper_id}/partition_{partition_id+1}.txt", 'w') as f:
                for centroid, point in partition:
                    f.write(f"{centroid}, {point}\n")
    

    def GetPairs(self, request, context):
        reducer_id = request.reducer_id - 1
        keys, values = list(zip(*self.partitions[reducer_id]))
        points = [mapper_pb2.Point(dim_val=list(v)) for v in values]
        return mapper_pb2.GetPairsResponse(keys=list(keys), values=points)