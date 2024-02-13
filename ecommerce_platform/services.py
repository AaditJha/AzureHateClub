import seller_pb2_grpc
import buyer_pb2_grpc
import notify_pb2_grpc
from notify_servicer import NotifyServicer
from seller_servicer import SellerServicer
from buyer_servicer import BuyerServicer

def register_all_services(server):
    seller_pb2_grpc.add_SellerServicer_to_server(SellerServicer(), server)
    buyer_pb2_grpc.add_BuyerServicer_to_server(BuyerServicer(), server)

def register_notify_service(server):
    notify_pb2_grpc.add_NotifyServicer_to_server(NotifyServicer(), server)