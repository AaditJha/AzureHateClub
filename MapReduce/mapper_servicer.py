import grpc
import mapper_pb2, mapper_pb2_grpc
import utils
from common import NUM_REDUCERS, MAPPER_DIR

class MapperServicer(mapper_pb2_grpc.MapperServicer):
    def __init__(self, mapper_id:int) -> None:
        self.mapper_id = mapper_id

    def Map(self, request:mapper_pb2.MapRequest, context:str) -> mapper_pb2.MapResponse:
        self.point_ids, self.centroids = utils.read_map_request(map_request=request)
        self.MapRoutine()
        self.Partition()
        self.CreateLocalFiles()
        response = utils.create_map_response(self.partitions)
        return response

    def Reduce(self, request, context):
        pass

    def MapRoutine(self) -> None:
        self.points = [utils.get_point_from_id(idx) for idx in self.point_ids]
        self.closest_centroid = [utils.get_closest_centroid(point, self.centroids) for point in self.points]
    
    # creates a list of tuple[int, list[float]]
    def Partition(self) -> None:
        self.partitions = [[] for _ in range(NUM_REDUCERS)]
        for i in range(len(self.closest_centroid)):
            self.partitions[self.closest_centroid[i] % NUM_REDUCERS].append((self.closest_centroid[i], self.points[i]))

    def CreateLocalFiles(self) -> None:
        for partition_id, partition in enumerate(self.partitions):
            with open(f"{MAPPER_DIR}/M{self.mapper_id}/partition_{partition_id+1}.txt", 'w') as f:
                for centroid, point in partition:
                    f.write(f"{centroid}, {point}\n")