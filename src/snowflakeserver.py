import sys
sys.path = ['..'] + sys.path

import zope
from twisted.internet import reactor

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport import TTwisted
from thrift.protocol import TBinaryProtocol

from lib.genpy.snowflake import Snowflake
from lib.genpy.snowflake.ttypes import *

import idworker

class SnowflakeServer(object):
    zope.interface.implements(Snowflake.Iface)
    
    def __init__(self, worker_id, datacenter_id):
        self.worker = idworker.IdWorker(worker_id, datacenter_id)
    
    def get_worker_id(self):
        return self.worker.get_worker_id()
    
    def get_datacenter_id(self):
        return self.worker.get_datacenter_id()
    
    def get_timestamp(self):
        return self.worker.get_timestamp()
    
    def get_id(self):
        return self.worker.get_id()

def print_usage():
    print 'python snowflakeserver.py <port> <worker_id> <datacenter_id>'

def main():
    if len(sys.argv) != 4:
        return print_usage()
    
    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])
    datacenter_id = int(sys.argv[3])
    reactor.listenTCP(port, TTwisted.ThriftServerFactory(
                                processor=Snowflake.Processor(SnowflakeServer(worker_id, datacenter_id)),
                                iprot_factory=TBinaryProtocol.TBinaryProtocolFactory()
    ))
    reactor.run()

if __name__ == '__main__':
    sys.exit(main())