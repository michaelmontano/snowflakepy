import time

TWEPOCH = 1288834974657

WORKER_ID_BITS = 5
DATACENTER_ID_BITS = 5
MAX_WORKER_ID = (1 << WORKER_ID_BITS) - 1
MAX_DATACENTER_ID = (1 << DATACENTER_ID_BITS) - 1
SEQUENCE_BITS = 12

WORKER_ID_SHIFT = SEQUENCE_BITS
DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS
TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS
SEQUENCE_MASK = (1 << SEQUENCE_BITS)

def gen_id(timestamp, datacenter_id, worker_id, sequence):
    return (timestamp - TWEPOCH) << TIMESTAMP_SHIFT | \
            (datacenter_id << DATACENTER_ID_SHIFT) | \
            (worker_id << WORKER_ID_SHIFT) | \
            sequence

class IdWorker(object):
    def __init__(self, worker_id, datacenter_id):
        assert worker_id >= 0 and worker_id <= MAX_WORKER_ID
        assert datacenter_id >= 0 and datacenter_id <= MAX_DATACENTER_ID
        
        self.worker_id = worker_id
        self.datacenter_id = datacenter_id
        
        self.sequence = 0
        self.last_timestamp = -1
        
        print "worker starting. timestamp left shift %d, datacenter_id bits %d, worker_id bits %d, sequence bits %d, worker_id %d" \
                %(TIMESTAMP_SHIFT, DATACENTER_ID_BITS, WORKER_ID_BITS, SEQUENCE_BITS, self.worker_id)
    
    def get_id(self):
        return self.next_id()
    
    def get_worker_id(self):
        return self.worker_id
    
    def get_datacenter_id(self):
        return self.datacenter_id
    
    def get_timestamp(self):
        return int(time.time()*1000)
    
    def next_id(self):
        timestamp = self.time_gen()
        
        if self.last_timestamp > timestamp:
            print "clock is moving backwards.  Rejecting requests until %d" %self.last_timestamp
            raise Exception, "Clock moved backwards.  Refusing to generate id for %d milliseconds" %(self.last_timestamp - timestamp)
        
        if self.last_timestamp == timestamp:
            self.sequence = (self.sequence + 1) % SEQUENCE_MASK
            if self.sequence == 0:
                timestamp = self.til_next_millis(self.last_timestamp)
        else:
            self.sequence = 0
        
        self.last_timestamp = timestamp
        return gen_id(timestamp, self.datacenter_id, self.worker_id, self.sequence)
    
    def til_next_millis(self, last_timestamp):
        timestamp = self.time_gen()
        while last_timestamp == timestamp:
            timestamp = self.time_gen()
        return timestamp
    
    def time_gen(self):
        return int(time.time()*1000)

class WakingIdWorker(IdWorker):
    def __init__(self, *args, **kwargs):
        super(WakingIdWorker, self).__init__(*args, **kwargs)
        self.sleeps = 0
    
    def til_next_millis(self, last_timestamp):
        timestamp = self.time_gen()
        while last_timestamp == timestamp:
            timestamp = self.time_gen()
            self.sleeps += 1
        return timestamp