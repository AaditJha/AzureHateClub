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
        print('Closing Notification Server...')
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

if __name__ == "__main__":
    print()
    buyer = Buyer()
    # buyer.search_product('iphon', shared_pb2.Category.Any)
    product_id = input('Enter product id to buy: ')
    # buyer.rate_product(product_id, 2)
    # buyer.buy_product(product_id,2)
    buyer.add_to_wishlist(product_id)
    buyer.notification_server.wait_for_termination()