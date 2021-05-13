# Map Reduce

This is my Map Reduce implementation in Python 3. It includes functionality for Word Count and Inverted Index. All network communication is over RPCs using the XML-RPC Python library.

Output files are included from testing along with a lab report.

### Command Line Arguments Available:
- -w (--word_count)
  - Run word count
- -i (--inverted_index)
  - Run inverted index
- -a (--address)
  - Local address
- -p (--port)
  - Local start port
- -m (--mappers)
  - Number of mappers to start
- -r (--reducers)
  - Number of reducers to start
- -f (--input_file)
  - Input file to be used for word count and one of the inverted index files
- -f2 (--input_file2)
  - Input file to be used for inverted inverted index
