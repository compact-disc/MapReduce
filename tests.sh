#!/bin/sh

# Word Count Tests
python3 main_node.py -f input1.txt -m 3 -r 3 -a localhost -p 34566 -w
sleep 1

python3 main_node.py -f input1.txt -m 1 -r 1 -a localhost -p 34566 -w
sleep 1

python3 main_node.py -f input2.txt -m 2 -r 2 -a localhost -p 34566 -w
sleep 1

python3 main_node.py -f input-long.txt -m 5 -r 5 -a localhost -p 34566 -w
sleep 1

# Inverted Index Tests
python3 main_node.py -f input1.txt -f2 input2.txt -i -a localhost -p 34566
sleep 1

python3 main_node.py -f input1.txt -f2 input2.txt -i -a localhost -p 34566 -r 3
sleep 1

python3 main_node.py -f input2.txt -f2 input1.txt -i -a localhost -p 34566 -r 2
sleep 1

python3 main_node.py -f input2.txt -f2 input1.txt -i -a localhost -p 34566
sleep 1

