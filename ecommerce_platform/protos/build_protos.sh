#!/bin/bash

# Build seller.proto
python -m grpc_tools.protoc -I. --python_out=../ --grpc_python_out=../ shared.proto
python -m grpc_tools.protoc -I. --python_out=../ --grpc_python_out=../ seller.proto
python -m grpc_tools.protoc -I. --python_out=../ --grpc_python_out=../ buyer.proto
python -m grpc_tools.protoc -I. --python_out=../ --grpc_python_out=../ notify.proto