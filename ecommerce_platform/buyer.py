import grpc
import buyer_pb2
import shared_pb2
import buyer_pb2_grpc

class Buyer:
    def __init__(self) -> None:
        self.channel = grpc.insecure_channel("localhost:42483")
        self.stub = buyer_pb2_grpc.BuyerStub(self.channel)
    
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
            qty=qty
        ))
        print(response.status)
        print('='*50)
    
    def rate_product(self, product_id, rating):
        response = self.stub.RateProduct(buyer_pb2.RateProductRequest(
            product_id=product_id,
            rating=rating
        ))
        print(response.status)
        print('='*50)
    
    def add_to_wishlist(self, product_id):
        response = self.stub.AddToWishlist(buyer_pb2.AddToWishlistRequest(
            product_id=product_id,
        ))
        print(response.status)
        print('='*50)

if __name__ == "__main__":
    print()
    buyer = Buyer()
    # buyer.search_product('iphon', shared_pb2.Category.Any)
    product_id = input('Enter product id to buy: ')
    # buyer.rate_product(product_id, 2)
    buyer.buy_product(product_id,2)
    # buyer.add_to_wishlist(product_id)