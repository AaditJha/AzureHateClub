import mapper_pb2, mapper_pb2_grpc
import linecache

def create_map_response(partitions: list[tuple[int, list[float]]]) -> mapper_pb2.MapResponse:
    map_response = mapper_pb2.MapResponse()

    for partition in partitions:
        for centroid_id, point_values in partition:
            key_value = map_response.pairs.add()
            key_value.closest_centroid = centroid_id
            point = key_value.point
            point.dim_val.extend(point_values)
            
    return map_response

def read_map_response(map_response: mapper_pb2.MapResponse) -> list[tuple[int, list[float]]]:
    partition = []

    for key_value in map_response.pairs:
        centroid_id = key_value.closest_centroid
        point_values = list(key_value.point.dim_val)
        partition.append((centroid_id, point_values))

    return partition

# print(utils.get_point_from_id(0, 'MapReduce/Data/Input/small_data.txt'))

# print(linecache.getline(filename='Data/Input/small_data.txt', lineno=1).split(', '))

B = [[(1, [2.0, -5.0]), (1, [3.0, -3.0]), (1, [1.0, -1.0]), (1, [-3.0, -2.0]), (0, [4.0, 3.0])], [(1, [-4.0, -2.0]), (1, [3.0, -1.0]), (1, [4.0, -4.0]), (1, [-3.0, -3.0]), (0, [-3.0, 4.0])]]

res = create_map_response(B)
print(res)
C = read_map_response(res)

print(C)