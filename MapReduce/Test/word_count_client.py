import grpc
import word_count_pb2
import word_count_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = word_count_pb2_grpc.WordCountStub(channel)

    text = "Hello world hello"
    print(f"Text: {text}")

    response = stub.Map(word_count_pb2.MapRequest(text=text))
    pairs = []
    for item in response:
        pairs.append(item)

    response = stub.Reduce(word_count_pb2.ReduceRequest(pairs=pairs))
    print("Word frequencies:")
    for word, count in response.counts.items():
        print(f"{word}: {count}")

if __name__ == '__main__':
    run()
