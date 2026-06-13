# dead-letter

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Zero dependencies](https://img.shields.io/badge/dependencies-0-brightgreen.svg)](#)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#)

A tiny **dead-letter queue** helper. Process items with bounded retries; anything
that exhausts its attempts is captured in a DLQ — with its failure reason and full
attempt history — instead of being lost or blocking your pipeline. Zero dependencies.

## Install
```bash
pip install dead-letter
```

## Quick start
```python
from dead_letter import DeadLetterQueue

def handle(msg):
    # raising = a failed attempt
    deliver(msg)

dlq = DeadLetterQueue(handle, max_attempts=3)

dlq.process(msg)                 # retries up to 3x, then dead-letters
res = dlq.process_batch(msgs)    # -> processed / dead_lettered counts

for dl in dlq.dead_letters:
    print(dl.item, dl.reason, dl.last_error, dl.history)
```

### Recover after a fix
```python
# dependency was down -> items dead-lettered. Once it's back:
dlq.requeue_all()    # re-attempts every dead letter; survivors leave the DLQ
```

### Drain to persist elsewhere
```python
for dl in dlq.drain():
    save_to_db(dl)
```

## Features
- ✅ Bounded retries with full attempt history
- ✅ Captures failure reason + last error per item
- ✅ `requeue_all()` to recover after fixing a dependency
- ✅ `drain()` to hand off / persist dead letters
- ✅ Thread-safe DLQ store
- ✅ **Zero dependencies**

## License
MIT © WCN Development Co
