import sys
sys.path = ['..'] + sys.path

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from lib.genpyblocking.snowflake import Snowflake
from lib.genpyblocking.snowflake.ttypes import *

def print_usage():
    print 'python test_client.py <port> <get_worker_id|get_datacenter_id|get_timestamp|get_id>'

def main():
    if len(sys.argv) != 3:
        return print_usage()
    
    port = int(sys.argv[1])
    service = sys.argv[2]
    
    transport = TSocket.TSocket('localhost', port)
    transport = TTransport.TFramedTransport(transport)
    prot = TBinaryProtocol.TBinaryProtocol(transport)
    client = Snowflake.Client(prot)
    transport.open()
    
    if service == 'get_worker_id':
        print client.get_worker_id()
    elif service == 'get_datacenter_id':
        print client.get_datacenter_id()
    elif service == 'get_timestamp':
        print client.get_timestamp()
    elif service == 'get_id':
        print client.get_id()
    else:
        print_usage()

if __name__ == '__main__':
    sys.exit(main())