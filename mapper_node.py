#!/usr/bin/env python3

# Import XML RPC server to receive data from the main_node to map
from xmlrpc.server import SimpleXMLRPCServer

# Import XML RPC client to then send the mapped data to the reducer
import xmlrpc.client

# Import group by to sort the data that will be separated by which node
from itertools import groupby


# Initialize the mapper with an address and port number
# Start an rpc server to get the data
def init_mapper(address, port, mapper_num):
    print("Mapper {} ready!".format(mapper_num))
    server = SimpleXMLRPCServer((address, port), allow_none=True)
    server.register_function(map_data_wordcount, "map_data_wordcount")
    server.register_function(map_data_inverted_index, "map_data_inverted_index")
    server.serve_forever()


# Map the data given from the main node
# Send it out based on the size of the word by hashing
def map_data_wordcount(data, mappers, reducers, address, port):
    values = []

    # The reducer node ports are numbered
    reducer_node = mappers + reducers - 1

    # Clean up any grammatical things in the word and map it
    # Then create a new array to store the newly mapped data
    # Store it with the respective mapper it will go to, this is done with the hashing function
    for word in data:
        word = word.replace(".", "")
        word = word.replace(",", "")
        mapped_word = word + ",1"
        node_num = hash_word(word, reducers)
        values.append(str(node_num) + ":" + str(mapped_word))

    # Sort the data by the reducer its going to and then group it together
    values.sort(key=lambda x: x[0])
    data = [list(i) for j, i in groupby(values, lambda a: a.split(":")[0])]

    print("Sending to reducers")
    # For each reducer, send out its designated data over RPC using an rpc client
    for i in range(reducers):
        with xmlrpc.client.ServerProxy("http://" + address + ":" + str(port + reducer_node)) as proxy:
            proxy.get_map_wordcount(data[i], mappers, i, address, port)


# Map the data given from the main node
# Send it out based on the size of the word by hashing
def map_data_inverted_index(data, mappers, reducers, address, port, doc_num):
    mapped = []
    cleaned = []

    # Clean up the words for commas and periods
    for word in data:
        word = word.replace(",", "")
        word = word.replace(".", "")
        cleaned.append(word)

    # Sort the data alphabetically
    cleaned.sort()
    # Group the data together that is the same (based on the word)
    data = [list(i) for j, i in groupby(cleaned)]

    # Go through each set of data (like words) and add its designation by hashing from word length
    for i in data:
        occur = len(i)
        word = i[0]
        hash_value = hash_word(word, reducers)
        mapped.append(str(hash_value) + ":" + str(word) + "," + str(doc_num) + "-" + str(occur))

    # Sort the data based on the reducer it is going to
    mapped.sort()
    chunks = [list(i) for j, i in groupby(mapped, lambda a: a.split(":")[0])]

    print("Sending to reducers")
    # Go through all of the data and sent it to its designated reducer
    for i in range(len(chunks)):
        with xmlrpc.client.ServerProxy("http://" + address + ":" + str(port + mappers + i)) as proxy:
            proxy.get_map_inverted_index(chunks[i], mappers, i, address, port)


# Return N - 1 reducer to send to based on the length % number of reducers
def hash_word(word, reducers):
    return len(word) % reducers
