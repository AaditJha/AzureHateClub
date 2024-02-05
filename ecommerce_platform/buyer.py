import grpc
import uuid
import register_seller_pb2_grpc, register_seller_pb2

def run(seller_id: uuid.UUID):
    channel = grpc.insecure_channel("localhost:42483")
    stub = register_seller_pb2_grpc.RegisterSellerStub(channel)
    response = stub.Register(register_seller_pb2.SellerRequest(seller_id=str(seller_id)))
    print(response.status)

if __name__ == "__main__":
    seller_id = uuid.uuid4()
    run(seller_id)