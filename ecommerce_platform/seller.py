import grpc
import uuid
from concurrent import futures
import seller_pb2_grpc, seller_pb2, shared_pb2
import services
import signal
import address

class Seller:
    def __init__(self) -> None:
        self.notification_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.channel = grpc.insecure_channel(f"{address.MARKET_IP}:{address.MARKET_PORT}")
        self.stub = seller_pb2_grpc.SellerStub(self.channel)
        self.seller_id = str(uuid.uuid4())
        services.register_notify_service(self.notification_server)
        # Register the signal handler
        signal.signal(signal.SIGINT, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGTERM, lambda signum, frame : self.handle_termination())
        port = self.notification_server.add_insecure_port(f"{address.SELLER_INTERNAL_IP}:0")
        self.notification_server.start()
        self.seller_addr = f"{address.SELLER_EXTERNAL_IP}:{port}"
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

def menu(seller:Seller):
    print(f'Welcome Seller: {seller.seller_addr}')
    seller.register_seller()
    print(f'Seller registered with ID: {seller.seller_id}')
    while(True):
        print('_'* 50)
        print('0. Register Seller')
        print('1. Add Product')
        print('2. Update Product')
        print('3. Delete Product')
        print('4. Display Products')
        print('5. Exit')
        choice = input('Enter your choice: ')
        if choice == '0':
            seller.register_seller()
        elif choice == '1':
            product_name = input('Enter product name: ')
            category = int(input('Enter category id (0-Electronics,1-Fashion,2-Others,3-Any): '))
            qty = int(input('Enter quantity: '))
            desc = input('Enter description: ')
            price = float(input('Enter price: '))
            print()
            seller.add_product(product_name, category, qty, desc, price)
        elif choice == '2':
            product_id = input('Enter product id: ')
            qty = int(input('Enter quantity: '))
            price = float(input('Enter price: '))
            print()
            seller.update_product(product_id, qty, price)
        elif choice == '3':
            product_id = input('Enter product id: ')
            print()
            seller.delete_product(product_id)
        elif choice == '4':
            print()
            seller.get_products()
        elif choice == '5':
            break
        else:
            print('Invalid choice')
    seller.handle_termination()

if __name__ == "__main__":
    seller = Seller()
    menu(seller)