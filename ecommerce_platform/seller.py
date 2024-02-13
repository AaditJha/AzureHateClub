import grpc
import uuid
from concurrent import futures
import seller_pb2_grpc, seller_pb2, shared_pb2
import services
import signal


class Seller:
    def __init__(self) -> None:
        self.notification_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.channel = grpc.insecure_channel("localhost:42483")
        self.stub = seller_pb2_grpc.SellerStub(self.channel)
        self.seller_id = str(uuid.uuid4())
        services.register_notify_service(self.notification_server)
        # Register the signal handler
        signal.signal(signal.SIGINT, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGTERM, lambda signum, frame : self.handle_termination())
        ip = "[::]"
        port = self.notification_server.add_insecure_port(f"{ip}:0")
        self.notification_server.start()
        self.seller_addr = f"{ip}:{port}"
        print(f'Notification server started on {self.seller_addr}')
    
    def handle_termination(self):
        print('Closing Notification Server...')
        self.notification_server.stop(0)

    def register_seller(self):
        seller_id = self.seller_id
        response = self.stub.RegisterSeller(seller_pb2.SellerRequest(seller_id=seller_id,seller_addr=self.seller_addr))
        print(response.status)
        print('='*50)

    def add_product(self, product_name: str, category: shared_pb2.Category, qty: int, desc: str, price: float):
        response = self.stub.AddProduct(seller_pb2.SellerProductRequest(
            seller_addr=self.seller_addr,
            seller_id=self.seller_id,
            product_name=product_name,
            category=category,
            qty=qty,
            desc=desc,
            price=price
        ))
        print(response.status)
        print('='*50)

    def update_product(self, product_id: str, qty : int, price: float):
        response = self.stub.UpdateProduct(seller_pb2.SellerUpdateProductRequest(
            seller_addr=self.seller_addr,
            seller_id=self.seller_id,
            product_id=product_id,
            price=price,
            qty=qty
        ))
        print(response.status)
        print('='*50)


    def delete_product(self, product_id: str):
        response = self.stub.DeleteProduct(seller_pb2.SellerDeleteProductRequest(
            seller_addr=self.seller_addr,
            seller_id=self.seller_id,
            product_id=product_id
        ))
        print(response.status)
        print('='*50)


    def get_products(self):
        response = self.stub.DisplaySellerProducts(seller_pb2.SellerRequest(seller_id=self.seller_id,seller_addr=self.seller_addr))
        print(f'Products from seller {response.seller_addr}')
        print('_'*50)
        for product in response.products:
            print(f'Product ID: {product.product_id}')
            print(f'Name: {product.product_name}')
            print(f'Price: ${product.price}')
            print(f'Category: {shared_pb2.Category.Name(product.category)}')
            print(f'Description: {product.desc}')
            print(f'Quantity Remaining: {product.qty}')
            print(f'Rating: {product.rating}/5')
            print('-'*50)
        print('='*50)

if __name__ == "__main__":
    seller = Seller()
    seller.register_seller()
    seller.add_product("Top 1", shared_pb2.Category.Fashion, 10, "V Neck deep cut, purple/blue/black 100% cotton", 1500.0)
    seller.add_product("iPhone 12", shared_pb2.Category.Electronics, 7, "Black iPhone 14 next gen blah blah", 150000.0)
    seller.get_products()
    product_id = input('Enter product id to update')
    seller.update_product(product_id, 5, 140000.0)
    seller.notification_server.wait_for_termination()