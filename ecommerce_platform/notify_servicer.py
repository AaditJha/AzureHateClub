import notify_pb2, notify_pb2_grpc, shared_pb2
from server_state import ServerStateSingleton

class NotifyServicer(notify_pb2_grpc.NotifyServicer):
    def __init__(self) -> None:
        super().__init__()
        self.server_state = ServerStateSingleton()
    
    def SendNotification(self, request, context):
        print('#'*50)
        print('[NOTIF] The Following Item has been updated: ')
        print(f'Product ID: {request.product_id}')
        print(f'Name: {request.product_name}')
        print(f'Price: ${request.price}')
        print(f'Category: {shared_pb2.Category.Name(request.category)}')
        print(f'Description: {request.desc}')
        print(f'Quantity Remaining: {request.qty}')
        print(f'Rating: {request.rating}/5')
        print('#'*50)
        return notify_pb2.NotificationResponse()