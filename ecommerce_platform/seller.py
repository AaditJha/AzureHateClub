import grpc
import uuid
import seller_pb2_grpc, seller_pb2
import enum

class Seller:
    def __init__(self) -> None:
        self.channel = grpc.insecure_channel("localhost:42483")
        self.stub = seller_pb2_grpc.SellerStub(self.channel)
        self.seller_id = uuid.uuid4()

def register_seller(seller: Seller):
    channel = grpc.insecure_channel("localhost:42483")
    stub = seller_pb2_grpc.SellerStub(channel)
    seller_id = str(seller.seller_id)
    response = seller.stub.RegisterSeller(seller_pb2.SellerRequest(seller_id=seller_id))
    print(response.status)

def add_product(seller: Seller, product_name: str, category: seller_pb2.Category, qty: int, desc: str, price: float):
    response = seller.stub.AddProduct(seller_pb2.SellerProductRequest(
        seller_id=str(seller.seller_id),
        product_name=product_name,
        category=category,
        qty=qty,
        desc=desc,
        price=price
    ))
    print(response.status)

def update_product(seller: Seller, product_id: str, qty : int, price: float):
    response = seller.stub.UpdateProduct(seller_pb2.SellerUpdateProductRequest(
        seller_id=str(seller.seller_id),
        product_id=product_id,
        price=price,
        qty=qty
    ))
    print(response.status)

def delete_product(seller: Seller, product_id: str):
    response = seller.stub.DeleteProduct(seller_pb2.SellerDeleteProductRequest(
        seller_id=str(seller.seller_id),
        product_id=product_id
    ))
    print(response.status)

if __name__ == "__main__":
    seller = Seller()
    register_seller(seller)
    add_product(seller, "Kurti", "FASHION", 10, "V Neck deep cut, purple/blue/black 100% cotton", 1500.0)
    product_id = input("Enter the product id to delete: ")
    # update_product(seller, product_id, 20, 2000.0)
    delete_product(seller, product_id)
    
