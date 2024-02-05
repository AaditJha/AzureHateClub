import register_seller_pb2
import register_seller_pb2_grpc
import uuid
from server_state import ServerStateSingleton

# MARKET <-> SELLER Services
class RegisterSellerServicer(register_seller_pb2_grpc.RegisterSellerServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()

    def Register(self, request, context):
        client_ip_port = context.peer()
        seller_id = uuid.UUID(request.seller_id)
        seller_ids = self.server_state.state.get('seller_ids', [])
        if seller_id in seller_ids:
            return register_seller_pb2.RegisterResponse(
                status="FAIL: Seller already registered.",
            )
        seller_ids.append(seller_id)
        self.server_state.save_to_state('seller_ids', seller_ids)
        print(f"Seller join request from : {client_ip_port}, uuid= {seller_id}")
        return register_seller_pb2.RegisterResponse(
            status="SUCCESS",
        )

def register_all_services(server):
    register_seller_pb2_grpc.add_RegisterSellerServicer_to_server(RegisterSellerServicer(), server)
    