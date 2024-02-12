import buyer_pb2
import buyer_pb2_grpc
from server_state import ServerStateSingleton

class BuyerServicer(buyer_pb2_grpc.BuyerServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()

    def BuyProduct(self, request, context):
        if request.qty < 1:
            return buyer_pb2.RateProductResponse(
                status="FAIL: Invalid quantity",
            )
        client_ip_port = context.peer()
        product_id = request.product_id
        qty = request.qty
        all_products = self.server_state.state.get('items',{})
        product = all_products.get(product_id,None)
        if product is None:
            return buyer_pb2.RateProductResponse(
                status="FAIL: Product not found.",
            )
        if qty > product['quantity']:
            return buyer_pb2.RateProductResponse(
                status="FAIL: Insufficient quantity.",
            )
        product['quantity'] -= qty
        all_products[product_id] = product
        self.server_state.save_to_state('items',all_products)
        print(f'Buy request {qty} of {product_id}, from {client_ip_port}')
        return buyer_pb2.RateProductResponse(
            status="SUCCESS",
        )


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

        
