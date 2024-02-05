import grpc
import uuid
import seller_pb2_grpc, seller_pb2

def run(seller_id: uuid.UUID):
    channel = grpc.insecure_channel("localhost:42483")
    stub = seller_pb2_grpc.SellerStub(channel)
    response = stub.RegisterSeller(seller_pb2.SellerRequest(seller_id=str(seller_id)))
    print(response.status)

if __name__ == "__main__":
    seller_id = uuid.uuid4()
    run(seller_id)