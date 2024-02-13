import grpc
import buyer_pb2
import shared_pb2
import buyer_pb2_grpc
from concurrent import futures
import services
import signal

class Buyer:
    def __init__(self) -> None:
        self.notification_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.channel = grpc.insecure_channel("localhost:42483")
        self.stub = buyer_pb2_grpc.BuyerStub(self.channel)
        services.register_notify_service(self.notification_server)
        # Register the signal handler
        signal.signal(signal.SIGINT, lambda signum, frame : self.handle_termination())
        signal.signal(signal.SIGTERM, lambda signum, frame : self.handle_termination())
        ip = "[::]"
        port = self.notification_server.add_insecure_port(f"{ip}:0")
        self.notification_server.start()
        self.buyer_addr = f"{ip}:{port}"
        print(f'Notification server started on {self.buyer_addr}')
    
    def handle_termination(self):
        print('\nClosing Notification Server...')
        self.notification_server.stop(0)

    def search_product(self, product_name, category):
        response = self.stub.SearchProduct(buyer_pb2.SearchProductRequest(
            product_name=product_name,
            category=category
        ))
        for product in response.products:
            print(f'Product ID: {product.product_id}')
            print(f'Name: {product.product_name}')
            print(f'Price: ${product.price}')
            print(f'Category: {shared_pb2.Category.Name(product.category)}')
            print(f'Description: {product.desc}')
            print(f'Quantity Remaining: {product.qty}')
            print(f'Rating: {product.rating}/5')
            print(f'Seller: {product.seller_addr}')
            print('-'*50)
        print('='*50)

    def buy_product(self, product_id, qty):
        response = self.stub.BuyProduct(buyer_pb2.BuyProductRequest(
            product_id=product_id,
            buyer_addr=self.buyer_addr,
            qty=qty,
        ))
        print(response.status)
        print('='*50)
    
    def rate_product(self, product_id, rating):
        response = self.stub.RateProduct(buyer_pb2.RateProductRequest(
            product_id=product_id,
            buyer_addr=self.buyer_addr,
            rating=rating,
        ))
        print(response.status)
        print('='*50)
    
    def add_to_wishlist(self, product_id):
        response = self.stub.AddToWishlist(buyer_pb2.AddToWishlistRequest(
            product_id=product_id,
            buyer_addr=self.buyer_addr,
        ))
        print(response.status)
        print('='*50)

def menu(buyer: Buyer):
    print(f'Welcome Buyer: {buyer.buyer_addr}')
    while(True):
        print('_'* 50)
        print('1. Search Product')
        print('2. Buy Product')
        print('3. Rate Product')    
        print('4. Add to Wishlist')
        print('5. Exit')
        choice = input('Enter your choice: ')
        if choice == '1':
            product_name = input('Enter product name: ')
            category = int(input('Enter category id (0-Electronics,1-Fashion,2-Others,3-Any): '))
            buyer.search_product(product_name, category)
        elif choice == '2':
            product_id = input('Enter product id: ')
            qty = int(input('Enter quantity: '))
            buyer.buy_product(product_id, qty)
        elif choice == '3':
            product_id = input('Enter product id: ')
            rating = int(input('Enter rating (0-5): '))
            buyer.rate_product(product_id, rating)
        elif choice == '4':
            product_id = input('Enter product id: ')
            buyer.add_to_wishlist(product_id)
        elif choice == '5':
            break
        else:
            print('Invalid choice')
    buyer.handle_termination()

if __name__ == "__main__":
    print()
    buyer = Buyer()
    menu(buyer)