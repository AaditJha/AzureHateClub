import grpc
from concurrent import futures
import signal
from services import register_all_services
from server_state import ServerStateSingleton
import address

def handle_termination(server):
    print('\nClosing Server...')
    server_state = ServerStateSingleton()
    server_state.save_state()
    server.stop(0)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    register_all_services(server)

    # Register the signal handler
    signal.signal(signal.SIGINT, lambda signum, frame : handle_termination(server))
    signal.signal(signal.SIGTERM, lambda signum, frame : handle_termination(server))

    server.add_insecure_port(f"{address.MARKET_INTERNAL_IP}:{address.MARKET_PORT}")
    server.start()
    print(f'Listening on {address.MARKET_INTERNAL_IP}:{address.MARKET_PORT}')
    server.wait_for_termination()


if __name__ == "__main__":
    serve()


