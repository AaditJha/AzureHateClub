import grpc
from concurrent import futures
import word_count_pb2
import word_count_pb2_grpc
from collections import defaultdict

class WordCountServicer(word_count_pb2_grpc.WordCountServicer):
    def Map(self, request, context):
        words = request.text.split()
        counts = defaultdict(int)
        for word in words:
            counts[word.lower()] += 1
        for word, count in counts.items():
            yield word_count_pb2.MapResponse(pairs=[word_count_pb2.KeyValue(key=word, value=str(count))])

    def Reduce(self, request, context):
        counts = defaultdict(int)
        for pair in request.pairs:
            counts[pair.key] += int(pair.value)
        return word_count_pb2.ReduceResponse(counts=counts)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    word_count_pb2_grpc.add_WordCountServicer_to_server(WordCountServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()