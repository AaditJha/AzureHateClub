import buyer_pb2
import buyer_pb2_grpc
import shared_pb2
from server_state import ServerStateSingleton
from fuzzywuzzy import fuzz

class BuyerServicer(buyer_pb2_grpc.BuyerServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()

    def fuzzy_search(self, product_name, all_products):
        product_name = product_name.lower()
        if product_name == '':
            return all_products
        products = {}
        for product_id, product in all_products.items():
            if fuzz.partial_ratio(product_name, product['product_name'].lower()) > 85:
                products[product_id] = product
        return products
    
    def filter_category(self, product_category, products):
        if product_category == shared_pb2.Category.Any:
            return products
        filtered_products = {}
        for product_id, product in products.items():
            if product['category'] == product_category:
                filtered_products[product_id] = product
        return filtered_products
    
    def compute_rating(self, product_id):
        ratings = self.server_state.state.get('ratings', {})
        product_ratings = ratings.get(product_id, {})
        rating = 0.0
        for product_rating in product_ratings.values():
            rating += product_rating
        if len(product_ratings) > 0:
            rating /= len(product_ratings)
        return rating

    def SearchProduct(self, request, context):
        product_name = request.product_name
        product_category = request.category
        all_products = self.server_state.state.get('items',{})
        products = self.fuzzy_search(product_name, all_products)
        products = self.filter_category(product_category, products)
        product_details = []
        seller_addrs = self.server_state.state.get('seller_addr',{})
        for product_id, product in products.items():
            product_details.append(buyer_pb2.BuyerProductDetails(
                product_id=product_id,
                desc=product['description'],
                qty=product['quantity'],
                rating=self.compute_rating(product_id),
                product_name=product['product_name'],
                category=product['category'],
                price=product['price_per_unit'],
                seller_addr=seller_addrs.get(product['seller_id'],'')
            ))
        return buyer_pb2.BuyerProductResponse(
            products=product_details,
        )


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

        
