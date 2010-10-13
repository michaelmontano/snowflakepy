import sys
sys.path = ['..'] + sys.path

from src import idworker
import time

WORKER_MASK = 0x000000000001F000
DATACENTER_MASK = 0x00000000003E0000
TIMESTAMP_MASK = 0xFFFFFFFFFFC00000

def test_generate_id():
    worker = idworker.IdWorker(1, 1)
    tid = worker.next_id()
    assert tid > 0

def test_generate_timestamp():
    worker = idworker.IdWorker(1, 1)
    t = int(time.time()*1000)
    assert (worker.get_timestamp() - t) < 50

def test_get_worker_id():
    worker = idworker.IdWorker(1, 1)
    assert worker.get_worker_id() == 1

def test_get_datacenter_id():
    worker = idworker.IdWorker(1, 1)
    assert worker.get_datacenter_id() == 1

def test_mask_worker_id():
    worker_id = 0x1F
    datacenter_id = 0
    worker = idworker.IdWorker(worker_id, datacenter_id)
    tid = worker.next_id()
    assert (tid & WORKER_MASK) >> idworker.WORKER_ID_SHIFT == worker_id

def test_mask_datacenter_id():
    worker_id = 0
    datacenter_id = 0x1F
    worker = idworker.IdWorker(worker_id, datacenter_id)
    tid = worker.next_id()
    assert (tid & DATACENTER_MASK) >> idworker.DATACENTER_ID_SHIFT == datacenter_id

def test_mask_timestamp():
    t = int(time.time()*1000)
    worker_id = 1
    datacenter_id = 1
    worker = idworker.IdWorker(worker_id, datacenter_id)
    tid = worker.next_id()
    assert ((tid & TIMESTAMP_MASK) >> idworker.TIMESTAMP_SHIFT) + idworker.TWEPOCH >= t

def test_roll_over_sequence_id():
    worker_id = 4
    datacenter_id = 4
    worker = idworker.IdWorker(worker_id, datacenter_id)
    start_sequence = 0xFFFFFF-20
    end_sequence = 0xFFFFFF+20
    worker.sequence = start_sequence
    for i in range(start_sequence, end_sequence + 1):
        tid = worker.next_id()
        assert (tid & WORKER_MASK) >> idworker.WORKER_ID_SHIFT == worker_id

def test_generate_increasing_ids():
    worker = idworker.IdWorker(1, 1)
    last_id = 0
    for i in range(100):
        tid = worker.next_id()
        assert tid > last_id
        last_id = tid

def test_generate_one_million_ids_quickly():
    worker = idworker.IdWorker(31, 31)
    t1 = int(time.time()*1000)
    for i in range(100000):
        tid = worker.next_id()
        assert tid
    t2 = int(time.time()*1000)
    rate = 100000.0/(t2 - t1)
    print 'generated 100000 ids in %d ms, or %s ids/ms' %(t2 - t1, rate)
    assert rate > 100

def test_sleep_on_same_id():
    worker = idworker.WakingIdWorker(1, 1)
    worker.sequence = 4095
    tid1 = worker.next_id()
    worker.sequence = 4095
    tid2 = worker.next_id()
    assert worker.sleeps > 0

def test_generate_unique_ids():
    worker = idworker.IdWorker(31, 31)
    s = set()
    n = 200000
    for i in range(n):
        tid = worker.next_id()
        s.add(tid)
    assert len(s) == n