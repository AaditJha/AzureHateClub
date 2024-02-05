import grpc
import uuid
import seller_pb2_grpc, seller_pb2
import enum

def register_seller(seller_id: uuid.UUID):
    channel = grpc.insecure_channel("localhost:42483")
    stub = seller_pb2_grpc.SellerStub(channel)
    seller_id = str(seller_id)
    response = stub.RegisterSeller(seller_pb2.SellerRequest(seller_id=seller_id))
    print(response.status)

def add_product(seller_id: uuid.UUID, product_name: str, category: seller_pb2.Category, qty: int, desc: str, price: float):
    channel = grpc.insecure_channel("localhost:42483")
    stub = seller_pb2_grpc.SellerStub(channel)
    response = stub.AddProduct(seller_pb2.SellerItemRequest(
        seller_id=str(seller_id),
        product_name=product_name,
        category=category,
        qty=qty,
        desc=desc,
        price=price
    ))
    print(response.status)

if __name__ == "__main__":
    seller_id = uuid.uuid4()
    register_seller(seller_id)
    add_product(seller_id, "Laptop", "ELECTRONICS", 10, "Dell XPS 15", 1500.0)
