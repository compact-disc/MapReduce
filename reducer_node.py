#!/usr/bin/env python3

import time

# Import XML RPC server to receive data from the mapper nodes to reduce
from xmlrpc.server import SimpleXMLRPCServer

# Import XML RPC client to then send end signal to main
import xmlrpc.client

# Import OS to remove temporary save files
import os

# Import group by for like words
from itertools import groupby


# Initialize the reducer with an address and port
# Start an XML RPC server to get the data from the mappers, wait forever
def init_reducer(address, port, reducer_num):
    print("Reducer {} ready!".format(reducer_num))
    server = SimpleXMLRPCServer((address, port), allow_none=True)
    server.register_function(get_map_wordcount, "get_map_wordcount")
    server.register_function(get_map_inverted_index, "get_map_inverted_index")
    server.serve_forever()


# Get the mapper data and word count it
def get_map_wordcount(data, mappers, reducer_num, address, port):

    # Open a file to store the data and store it with it's reducer number
    # file name is also dynamic so there is one per each reducer
    file = open("reducer_" + str(reducer_num) + ".txt", "a")
    file.write(str(reducer_num) + "\n")
    for i in data:
        i = i.replace(str(reducer_num) + ":", "")
        file.write(str(i) + "-")
    file.write("\n")
    file.close()

    # Open back up the file and read in the data
    file = open("reducer_" + str(reducer_num) + ".txt", "r")
    file_data = file.read().splitlines()
    file.close()

    # Array to store the data to reduce
    to_reduce = []

    # If there is 2 times the amount of reducers line, then this reducer is finished getting data
    # Begin the reduce
    if len(file_data) == mappers * 2:
        # Go through the data from the file and split it up, add it to an array
        for i in range(1, len(file_data), 2):
            if i != len(file_data):
                to_reduce.extend(file_data[i].split("-"))

        # Go through the data that needs reduced, remove the mapping
        for i in range(len(to_reduce)):
            word = to_reduce[i]
            word = word.replace(",1", "")
            to_reduce[i] = word

        # Sort the data into key value pairs, this will show how many of each word is in the array
        reduced_data = {i: to_reduce.count(i) for i in to_reduce}

        # Print out the result of word counting for each word
        for key, value in reduced_data.items():
            if key != "":
                print(str(key) + " : " + str(value))

        # Remove the temporary data store used for getting data
        os.remove("reducer_" + str(reducer_num) + ".txt")

        # Mark this reducer as complete
        complete(address, port)


# Get the data from the mappers to run inverted index
def get_map_inverted_index(data, mappers, reducer_num, address, port):

    # Open a file to store the data and store it with it's reducer number
    # file name is also dynamic so there is one per each reducer
    file = open("reducer_" + str(reducer_num) + ".txt", "a")
    file.write(str(reducer_num) + "\n")
    for i in data:
        i = i.replace(str(reducer_num) + ":", "")
        file.write(str(i) + "|")
    file.write("\n")
    file.close()

    # Open back up the file and read in the data
    file = open("reducer_" + str(reducer_num) + ".txt", "r")
    file_data = file.read().splitlines()
    file.close()

    # Array to store the data to reduce
    # Dictionary to store the final reduced data
    to_reduce = []
    reduced = {}

    # If there is 2 times the amount of reducers line, then this reducer is finished getting data
    # Begin the reduce
    if len(file_data) == mappers * 2:

        # Read the data that was gotten by the file, split it up
        for i in range(1, len(file_data), 2):
            if i != len(file_data):
                to_reduce.extend(file_data[i].split("|"))

        # Sort the split data alphabetically
        to_reduce.sort()
        # Group the data into sub arrays
        data = [list(i) for j, i in groupby(to_reduce, lambda a: a.split(",")[0])]

        # Go through the data
        for i in data:
            # Was getting an error with extra slots, this is to catch some blank data
            if i[0] == "":
                continue

            # If the length is 2 (shows in both files)
            # Then we can get the word, and both files occurrences
            # Add that result to our final dictionary
            # Else do the same thing but for the only appearing in one file
            if len(i) >= 2:
                part1 = i[0].split(",")
                part2 = i[1].split(",")
                word = part1[0]
                occ1 = part1[1]
                occ2 = part2[1]
                reduced[word] = "(" + occ1 + ") (" + occ2 + ")"
            else:
                part1 = i[0].split(",")
                word = part1[0]
                occ1 = part1[1]
                reduced[word] = "(" + occ1 + ")"

        # For all the key value pairs created, print out the result
        for key, value in reduced.items():
            print(str(key) + " " + str(value))

        # Remove the temporary reducer store
        os.remove("reducer_" + str(reducer_num) + ".txt")

        # Mark to the main that is reducer is complete
        complete(address, port)


# Function to send a signal to the main that the reducer is complete
def complete(address, port):
    print("Reducer Complete!")
    with xmlrpc.client.ServerProxy("http://" + address + ":" + str(port - 1)) as proxy:
        proxy.complete()
