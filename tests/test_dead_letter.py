import pytest
from dead_letter import DeadLetterQueue, DeadLetter

def test_success_no_dead_letter():
    seen=[]
    dlq=DeadLetterQueue(lambda x: seen.append(x), max_attempts=3)
    assert dlq.process("ok") is True
    assert len(dlq)==0
    assert seen==["ok"]

def test_failure_dead_letters_after_attempts():
    attempts=[]
    def h(x): attempts.append(x); raise ValueError("boom")
    dlq=DeadLetterQueue(h, max_attempts=3)
    assert dlq.process("bad") is False
    assert len(attempts)==3              # tried exactly max_attempts
    assert len(dlq)==1
    dl=dlq.dead_letters[0]
    assert dl.item=="bad"
    assert dl.attempts==3
    assert "ValueError" in dl.last_error
    assert len(dl.history)==3

def test_recovers_before_exhausting():
    calls=[]
    def h(x):
        calls.append(x)
        if len(calls)<2: raise RuntimeError("transient")
    dlq=DeadLetterQueue(h, max_attempts=3)
    assert dlq.process("retryme") is True   # fails once then succeeds
    assert len(dlq)==0
    assert len(calls)==2

def test_batch_counts():
    def h(x):
        if x%2==0: raise ValueError("even bad")
    dlq=DeadLetterQueue(h, max_attempts=1)
    res=dlq.process_batch([1,2,3,4,5])
    assert res.processed==3            # 1,3,5
    assert res.dead_lettered==2        # 2,4
    assert len(dlq)==2

def test_requeue_recovers_when_handler_fixed():
    state={"fail":True}
    def h(x):
        if state["fail"]: raise RuntimeError("down")
    dlq=DeadLetterQueue(h, max_attempts=1)
    dlq.process("x"); assert len(dlq)==1
    state["fail"]=False                # "fix" the dependency
    res=dlq.requeue_all()
    assert res.retried==1
    assert len(dlq)==0

def test_requeue_still_failing_redeadletters():
    dlq=DeadLetterQueue(lambda x:(_ for _ in ()).throw(ValueError("always")), max_attempts=1)
    dlq.process("x")
    res=dlq.requeue_all()
    assert res.dead_lettered==1
    assert len(dlq)==1

def test_drain_empties():
    dlq=DeadLetterQueue(lambda x:(_ for _ in ()).throw(ValueError("x")), max_attempts=1)
    dlq.process("a"); dlq.process("b")
    drained=dlq.drain()
    assert len(drained)==2
    assert len(dlq)==0

def test_invalid_max_attempts():
    with pytest.raises(ValueError):
        DeadLetterQueue(lambda x:x, max_attempts=0)
