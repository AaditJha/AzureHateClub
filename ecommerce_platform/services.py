import seller_pb2
import seller_pb2_grpc
import uuid
from server_state import ServerStateSingleton

# MARKET <-> SELLER Services
class SellerServicer(seller_pb2_grpc.SellerServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()

    def RegisterSeller(self, request, context):
        client_ip_port = context.peer()
        seller_id = uuid.UUID(request.seller_id)
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
        client_ip_port = context.peer()
        seller_id = uuid.UUID(request.seller_id)
        seller_ids = self.server_state.state.get('seller_ids', [])
        if seller_id not in seller_ids:
            return seller_pb2.RegisterResponse(
                status="FAIL: Seller not registered.",
            )
        product_id = uuid.uuid4()
        product = {
            'product_name' : request.product_name,
            'category' : request.category,
            'quantity' : request.qty,
            'description' : request.desc,
            'price_per_unit' : request.price,
            'seller_id' : seller_id,
            'product_id' : product_id
        }
        items = self.server_state.state.get('items', [])
        self.server_state.save_to_state('items', items + [product])
        ratings = self.server_state.state.get('ratings', {})
        ratings[str(product_id)] = {}
        self.server_state.save_to_state('ratings', ratings)
        print(f'Sell item request from {client_ip_port}')
        return seller_pb2.RegisterResponse(
            status="SUCCESS",
        )

def register_all_services(server):
    seller_pb2_grpc.add_SellerServicer_to_server(SellerServicer(), server)
    