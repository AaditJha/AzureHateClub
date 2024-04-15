import utils
import argparse
from address import MASTER_IP_PORT, MAPPER_IP_PORT


class Mapper:
    def __init__(self, ip_port:str) -> None:
        self.ip_port = ip_port

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Mapper program for running the MapReduce K-Means algorithm.")
    parser.add_argument("--id", type=int, help="ID of mapper, in range [1, ..., M]")
    args = parser.parse_args()
    Mapper(ip_port=MAPPER_IP_PORT[args.id])