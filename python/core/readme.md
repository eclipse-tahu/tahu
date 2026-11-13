# To generate the base protobuf sparkplug_b Python library
protoc -I=../../sparkplug_b/ --python_out=. ../../sparkplug_b/sparkplug_b.proto 

# Run the Python timestamp regression tests from the repository root
PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python python -m unittest discover -s python/tests -v

The pure-Python protobuf implementation supports the checked-in older generated
descriptors when running these tests with newer protobuf runtimes.
