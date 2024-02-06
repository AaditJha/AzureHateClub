import buyer_pb2
import buyer_pb2_grpc
from server_state import ServerStateSingleton

class BuyerServicer(buyer_pb2_grpc.BuyerServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()
    
    def RateProduct(self, request, context):
        if request.rating < 1 or request.rating > 5:
            return buyer_pb2.RateProductResponse(
                status="FAIL: Invalid rating",
            )
        client_ip_port = context.peer()
        product_id = request.product_id
        all_product_ratings = self.server_state.state.get('ratings', {})
        product_ratings = all_product_ratings.get(product_id, None)
        if product_ratings is None:
            return buyer_pb2.RateProductResponse(
                status="FAIL: Product not found.",
            )
        if client_ip_port in product_ratings:
            return buyer_pb2.RateProductResponse(
                status="FAIL: Already rated",
            )
        product_ratings[client_ip_port] = request.rating
        all_product_ratings[product_id] = product_ratings
        self.server_state.save_to_state('ratings', all_product_ratings)
        print(f'{client_ip_port} rated {product_id} with {request.rating} stars.')
        return buyer_pb2.RateProductResponse(
            status="SUCCESS",
        )

        
