import grpc
import buyer_pb2
import buyer_pb2_grpc

class Buyer:
    def __init__(self) -> None:
        self.channel = grpc.insecure_channel("localhost:42483")
        self.stub = buyer_pb2_grpc.BuyerStub(self.channel)
    
    def rate_product(self, product_id, rating):
        response = self.stub.RateProduct(buyer_pb2.RateProductRequest(
            product_id=product_id,
            rating=rating
        ))
        print(response.status)
        print('='*50)

if __name__ == "__main__":
    buyer = Buyer()
    product_id = input('Enter product id to rate: ')
    buyer.rate_product(product_id, 2)