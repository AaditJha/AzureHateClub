import grpc
import reducer_pb2_pb2, reducer_pb2_grpc

class ReducerServicer(reducer_pb2_grpc.ReducerServicer):
    def Reduce(self, request, context):
        pass