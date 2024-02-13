import seller_pb2
import uuid
import seller_pb2_grpc
from server_state import ServerStateSingleton

# MARKET <-> SELLER Services
class SellerServicer(seller_pb2_grpc.SellerServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()
    
    def verify_seller(self, seller_id, seller_ip_port):
        seller_ids = self.server_state.state.get('seller_ids', [])
        if seller_id not in seller_ids:
            print('Seller not registered')
            return False
        seller_addr = self.server_state.state.get('seller_addr', {})
        seller_addr = seller_addr.get(seller_id, None)
        if seller_addr is None or seller_addr != seller_ip_port:
            print(seller_addr, seller_ip_port)
            print('Seller IP mismatch')
            return False
        return True

    def RegisterSeller(self, request, context):
        client_ip_port = request.seller_addr
        seller_id = request.seller_id
        seller_ids = self.server_state.state.get('seller_ids', [])
        if seller_id in seller_ids:
            return seller_pb2.RegisterResponse(
                status="FAIL: Seller already registered.",
            )
        seller_addr = self.server_state.state.get('seller_addr', {})
        seller_addr[request.seller_id] = client_ip_port
        seller_ids.append(seller_id)
        self.server_state.save_to_state('seller_ids', seller_ids)
        self.server_state.save_to_state('seller_addr', seller_addr)
        print(f"Seller join request from : {client_ip_port}, uuid= {seller_id}")
        return seller_pb2.RegisterResponse(
            status="SUCCESS",
        )
    
    def AddProduct(self, request, context):
        client_ip_port = request.seller_addr
        seller_id = request.seller_id
        if not self.verify_seller(seller_id, client_ip_port):
            return seller_pb2.RegisterResponse(
                status="FAIL: Credential Mismatch.",
            )
        product_id = str(uuid.uuid4())
        product = {
            'product_name' : request.product_name,
            'category' : request.category,
            'quantity' : request.qty,
            'description' : request.desc,
            'price_per_unit' : request.price,
            'seller_id' : seller_id,
        }
        items = self.server_state.state.get('items', {})
        items[product_id] = product
        self.server_state.save_to_state('items', items)
        ratings = self.server_state.state.get('ratings', {})
        ratings[product_id] = {}
        self.server_state.save_to_state('ratings', ratings)
        all_seller_items = self.server_state.state.get('seller_items', {})
        seller_items = all_seller_items.get(seller_id, [])
        seller_items.append(product_id)
        all_seller_items[seller_id] = seller_items
        self.server_state.save_to_state('seller_items', all_seller_items)
        print(f'Sell item request from {client_ip_port}')
        return seller_pb2.RegisterResponse(
            status="SUCCESS",
        )
    
    def UpdateProduct(self, request, context):
        client_ip_port = request.seller_addr
        seller_id = request.seller_id
        if not self.verify_seller(seller_id, client_ip_port):
            return seller_pb2.RegisterResponse(
                status="FAIL: Credential Mismatch.",
            )        
        items = self.server_state.state.get('items', [])
        if request.product_id not in items.keys():
            return seller_pb2.RegisterResponse(
                status="FAIL: Product not found.",
            )
        product = items[request.product_id]
        if product['seller_id'] != seller_id:
            return seller_pb2.RegisterResponse(
                status="FAIL: Not authorized to update product.",
            )
        product['price_per_unit'] = request.price
        product['quantity'] = request.qty
        items[request.product_id] = product
        self.server_state.save_to_state('items', items)
        print(f"Update product {request.product_id} request from {client_ip_port}")
        return seller_pb2.RegisterResponse(
            status="SUCCESS",
        )
    
    def DeleteProduct(self, request, context):
        client_ip_port = request.seller_addr
        seller_id = request.seller_id
        if not self.verify_seller(seller_id, client_ip_port):
            return seller_pb2.RegisterResponse(
                status="FAIL: Credential Mismatch.",
            )        
        items = self.server_state.state.get('items', [])
        if request.product_id not in items.keys():
            return seller_pb2.RegisterResponse(
                status="FAIL: Product not found.",
            )
        product = items[request.product_id]
        if product['seller_id'] != seller_id:
            return seller_pb2.RegisterResponse(
                status="FAIL: Not authorized to delete product.",
            )
        del items[request.product_id]
        ratings = self.server_state.state.get('ratings', None)
        if ratings is not None:
            del ratings[request.product_id]
        self.server_state.save_to_state('items', items)
        try:
            all_seller_items = self.server_state.state.get('seller_items', {})
            seller_items = all_seller_items.get(seller_id, [])
            print(seller_id,seller_items)
            seller_items.remove(request.product_id)
            all_seller_items[seller_id] = seller_items
            self.server_state.save_to_state('seller_items', all_seller_items)
        except ValueError:
            pass
        print(f"Delete product {request.product_id} request from {client_ip_port}")
        return seller_pb2.RegisterResponse(
            status="SUCCESS",
        )
    
    def compute_rating(self, product_id):
        ratings = self.server_state.state.get('ratings', {})
        product_ratings = ratings.get(product_id, {})
        rating = 0.0
        for product_rating in product_ratings.values():
            rating += product_rating
        if len(product_ratings) > 0:
            rating /= len(product_ratings)
        return rating
    
    def DisplaySellerProducts(self, request, context):
        client_ip_port = request.seller_addr
        seller_id = request.seller_id
        if not self.verify_seller(seller_id, client_ip_port):
            return seller_pb2.ProductResponse(
                seller_addr=client_ip_port,
                products=[],
            )
        
        items = self.server_state.state.get('items', [])
        seller_items = []
        all_seller_items = self.server_state.state.get('seller_items', {})
        seller_item_ids = all_seller_items.get(seller_id, [])
        for item_id in seller_item_ids:
            item = items[item_id]
            seller_items.append(seller_pb2.ProductDetails(
                    product_id=item_id,
                    product_name=item['product_name'],
                    category=item['category'],
                    qty=item['quantity'],
                    desc=item['description'],
                    price=item['price_per_unit'],
                    rating=self.compute_rating(item_id),
                ))
        return seller_pb2.ProductResponse(
            seller_addr=client_ip_port,
            products=seller_items,
        )
