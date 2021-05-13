#!/usr/bin/env python3

# Import os to kill the processes for the nodes
import os

# Used to give signal interrupts to the XML RPC's in the mappers and reducers
import signal

# Used to slow the while loop to check for an exit condition
import time

# Imported to shutdown the program when finished
import sys

# Import multiprocessing to start nodes in processes
from multiprocessing import Process

import threading

# Import parsing for command line arguments
import argparse

# Import method to start the mapper as a separate process
from mapper_node import init_mapper

# Import method to start the reducer as a separate process
from reducer_node import init_reducer

# Import XML RPC Client to send data to the mappers
import xmlrpc.client

# Import XML RPC server to receive data from the reducers
from xmlrpc.server import SimpleXMLRPCServer

# Arrays to store the mapper and reducer processes
mapper_processes = []
reducer_processes = []


def main():
    # Add an argument parser for configuration of running Map Reduce
    # Add the different argument and their default values
    parser = argparse.ArgumentParser(description="MapReduce")
    parser.add_argument("-w", "--word_count", action="store_true", default=True, help="Run word count on file")
    parser.add_argument("-i", "--inverted_index", action="store_true", default=False, help="Run inverted index")
    parser.add_argument("-a", "--address", type=str, default="localhost", help="Host names")
    parser.add_argument("-p", "--port", type=int, default=34560, help="Starting port number")
    parser.add_argument("-m", "--mappers", type=int, default=2, help="Number of mappers to spawn")
    parser.add_argument("-r", "--reducers", type=int, default=2, help="Number of reducers to spawn")
    parser.add_argument("-f", "--input_file", type=str, default="input1.txt", help="Input data file")
    parser.add_argument("-f2", "--input_file2", type=str, default="input2.txt", help="Second file for inverted index")

    # Parse the arguments that are passed into the file
    args = parser.parse_args()

    # Create the XML RPC server for waiting for the reducers to be finished
    server = SimpleXMLRPCServer((args.address, args.port - 1), allow_none=True)
    # Create an instance of the rpc server and pass the number of reducers to it
    server.register_instance(ReducerHandler(args.reducers))
    # Start the rpc server in another thread so we can continue other operations
    t = threading.Thread(target=start_rpc_listener, args=(server,))
    # Start the rpc server thread
    t.start()

    # If running inverted index, set word count to false and mappers to 2 (which is 1 per file)
    # Else if running word count, continue normally and print message
    if args.inverted_index:
        args.word_count = False
        args.mappers = 2
        print("Running Inverted Index!")
    else:
        args.inverted_index = False
        print("Running Word Count!")

    # Call the init_cluster function to create the cluster nodes
    init_cluster(args.address, args.port, args.mappers, args.reducers)
    print("Cluster Initialized")

    # Store the chunked data from the input files into arrays to send to nodes
    chunks = []
    chunks2 = []

    # Get the data in chunks from the input file(s)
    if args.word_count:
        # Read and split the data for word count
        chunks = read_and_split_data(args.input_file, args.mappers, args.inverted_index)
    elif args.inverted_index:
        # Read and chunk 2 separate input files for 2 different mappers only for inverted index
        chunks = read_and_split_data(args.input_file, 1, args.inverted_index)
        chunks2 = read_and_split_data(args.input_file2, 1, args.inverted_index)

    # Sending data to mappers based on address and port
    if args.word_count:
        # Send the chunked data to the mappers for word count
        send_data_to_mappers_wordcount(args.address, args.port, chunks, args.mappers, args.reducers)
        print("Sent to mappers!")
    elif args.inverted_index:
        # Send the chunked data from the 2 files to the 2 mappers for inverted index
        send_data_to_mappers_inverted_index(args.address, args.port, chunks, args.mappers, args.reducers, 0)
        send_data_to_mappers_inverted_index(args.address, args.port, chunks2, args.mappers, args.reducers, 1)
        print("Sent to mappers!")


# Method to start the XML RPC server in a separate thread in the background
def start_rpc_listener(server):
    # Start and run the XML RPC server
    server.serve_forever()


# Initialize the nodes in the cluster as separate processes
# Pass the address, port, and node number to them, each will have 0-n
def init_cluster(address, port, mappers, reducers):
    count = 0
    for i in range(mappers):
        p = Process(target=init_mapper, args=(address, port + count, count))
        p.start()
        mapper_processes.append(p.pid)
        count += 1

    for i in range(reducers):
        p = Process(target=init_reducer, args=(address, port + count, count))
        p.start()
        reducer_processes.append(p.pid)
        count += 1


# Read and split the data into chunks based on the number of mappers spawned
# This will evenly split the load across the mappers
def read_and_split_data(input_file, mappers, inverted_index):
    # Read the first file
    file = open(input_file, "r")
    data = file.read()
    file.close()

    words = data.split()
    length = len(words)

    chunks = [words[x:x + int(length / mappers)] for x in range(0, len(words), int(length / mappers))]

    print("Data read from input file(s)!")
    if inverted_index:
        return chunks[0]
    else:
        return chunks


# Send the chunked data to the mappers, this will be done over rpc and dynamically used to send to
# any number of mappers, this is done by incrementing the port numbers by 1 every time one is initialized
def send_data_to_mappers_wordcount(address, port, chunks, mappers, reducers):
    node_num = 0

    for chunk in chunks:
        with xmlrpc.client.ServerProxy("http://" + address + ":" + str(port + node_num)) as proxy:
            proxy.map_data_wordcount(chunk, mappers, reducers, address, port)
            node_num += 1


# Send the chunked data to the mappers, this will be done over rpc
# There is a a 1 file to 1 mapper ratio and 1 is sent to each after separated
def send_data_to_mappers_inverted_index(address, port, chunk, mappers, reducers, mapper_id):
    with xmlrpc.client.ServerProxy("http://" + address + ":" + str(port + mapper_id)) as proxy:
        proxy.map_data_inverted_index(chunk, mappers, reducers, address, port, mapper_id + 1)


# This class will hold the methods and counters to see if the reducers are finished
# When the number of reducers finished is equal to the number of reducers, the main node will shutdown
# This will cause the sub-nodes processes to be killed as well
class ReducerHandler:

    def __init__(self, reducers):
        self.num_complete = 0
        self.reducers = reducers

    def complete(self):
        self.num_complete += 1
        if self.num_complete == self.reducers:
            threading.Thread(target=close).start()
            return 1
        else:
            return 0


# Kill all of the nodes and clear the arrays holding their process id's
# This will be used to check and see that everything is finished from the main node
def close():
    for p in mapper_processes:
        os.kill(p, signal.SIGTERM)
    for p in reducer_processes:
        os.kill(p, signal.SIGTERM)

    mapper_processes.clear()
    reducer_processes.clear()

    os.kill(os.getpid(), signal.SIGTERM)


if __name__ == "__main__":
    main()
