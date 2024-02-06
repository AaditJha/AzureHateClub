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
    print('='*50)

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
    print('='*50)

def update_product(seller: Seller, product_id: str, qty : int, price: float):
    response = seller.stub.UpdateProduct(seller_pb2.SellerUpdateProductRequest(
        seller_id=str(seller.seller_id),
        product_id=product_id,
        price=price,
        qty=qty
    ))
    print(response.status)
    print('='*50)


def delete_product(seller: Seller, product_id: str):
    response = seller.stub.DeleteProduct(seller_pb2.SellerDeleteProductRequest(
        seller_id=str(seller.seller_id),
        product_id=product_id
    ))
    print(response.status)
    print('='*50)


def get_products(seller: Seller):
    response = seller.stub.DisplaySellerProducts(seller_pb2.SellerRequest(seller_id=str(seller.seller_id)))
    print(f'Products from seller {response.seller_addr}')
    print('_'*50)
    for product in response.products:
        print(f'Product ID: {product.product_id}')
        print(f'Name: {product.product_name}')
        print(f'Price: ${product.price}')
        print(f'Category: {seller_pb2.Category.Name(product.category)}')
        print(f'Description: {product.desc}')
        print(f'Quantity Remaining: {product.qty}')
        print(f'Rating: {product.rating}/5')
        print('-'*50)
    print('='*50)

if __name__ == "__main__":
    seller = Seller()
    register_seller(seller)
    add_product(seller, "Top", seller_pb2.Category.Fashion, 10, "V Neck deep cut, purple/blue/black 100% cotton", 1500.0)
    add_product(seller, "iPhone 15", seller_pb2.Category.Electronics, 7, "Black iPhone 14 next gen blah blah", 150000.0)
    get_products(seller)
    
