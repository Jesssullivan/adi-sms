"""Pipeline

One Sender loop runs at every level; Only the queue differs: LocalQueue for threads, ProcQueue for processes and
StreamQueue (a Valkey stream with a consumer group) for pods. Now *that* is pretty neat, dontcha think?
"""

import math
import multiprocessing as mp
import queue
import sys
import threading
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, Protocol

from . import fate
from .fate import Config, Gateway, Message, Summary

type Snap = dict[str, float]
type Report = tuple[str, Summary, bool]  # a sender's name, its own Summary so far and whether it has stopped

# the most sender pods the controller can ask for, do not use more than this plz, not a lot of wiggleroom in there
P_MAX = 10

# ack, count and delete as one script
# Hey! This is an atomic;  atomicity is critical to making this all work.
COMMIT = (Path(__file__).parent / "commit.lua").read_text()


class Clock:  # real time; the tests just swap in a virtual clock with the same five methods
    def __init__(self) -> None:
        self.lock, self.t0 = threading.RLock(), time.monotonic()  # every local wait is on a condition of this lock

    def now(self) -> float:  # seconds since the clock was made
        return time.monotonic() - self.t0

    def sleep(self, d: float) -> None:
        time.sleep(d)

    def condition(self) -> threading.Condition:
        return threading.Condition(self.lock)

    def wait(self, cv: threading.Condition, ready: Callable[[], object], timeout: float | None = None) -> bool:
        return bool(cv.wait_for(ready, timeout))  # call it with the lock held

    def spawn(self, f: Callable[..., object], *args: object) -> threading.Thread:
        t = threading.Thread(target=f, args=args, daemon=True)
        t.start()
        return t


class Queue(Protocol):
    """The interface for data between the producer and the senders; this structure
    describes my whiteboard drawing's rectangle queue from my initial whiteboard session.

    The "Static duck typing" pattern in python with `...` is a pattern I've only recently begin to adopt, pretty cool.

    This allows us to cleanly type queue ops without immplimentation at an interface level!

    LocalQueue (threads), ProcQueue (processes) and StreamQueue (pods) do not inherit from Queue;
    instead they match it by having the same four methods, which mypy can check.
    """

    def put(self, msg: Message | None) -> None: ...  # blocks while C are queued (backpressure); None stops one sender
    def get(self, name: str) -> Message | None: ...  # one sender takes one message, atomically; None means stop
    def done(self, name: str, msg: Message, s: Summary) -> bool: ...  # commit one message's Summary; False: not new
    def report(self, name: str, total: Summary, last: bool) -> None: ...  # a sender's own Summary, for the monitor


class LocalQueue:  # threads: a queue.Queue of at most C
    def __init__(self, clock: Clock, capacity: int) -> None:
        self.clock, self.items, self.stats = clock, queue.Queue[Message | None](capacity), dict[str, Report]()
        self.room, self.ready, self.news = clock.condition(), clock.condition(), clock.condition()
        self.inflight = self.peak_inflight = self.peak_queued = self.lasts = self.excess = 0  # excess: stops due
        self.sojourn_us = self.commits = 0  # due to commit, summed over the committed messages

    def put(self, msg: Message | None) -> None:
        with self.clock.lock:
            self.clock.wait(self.room, lambda: not self.items.full())
            self.items.put_nowait(msg)
            self.peak_queued = max(self.peak_queued, self.items.qsize())
            self.ready.notify()

    def get(self, name: str) -> Message | None:
        with self.clock.lock:
            self.clock.wait(self.ready, lambda: self.excess or not self.items.empty())
            if self.excess:
                self.excess -= 1
                return None
            msg = self.items.get_nowait()
            self.inflight += msg is not None
            self.peak_inflight = max(self.peak_inflight, self.inflight)
            self.room.notify()
            return msg

    def done(self, name: str, msg: Message, s: Summary) -> bool:
        with self.clock.lock:
            self.inflight -= 1
            self.commits += 1
            self.sojourn_us += round(self.clock.now() * 1e6) - msg.created_us
            return True

    def stop(self, k: int) -> int:  # k senders stop at their next get, before any message; k < 0 cancels; returns
        with self.clock.lock:  # the senders to start: those the cancelled stops could not cover
            self.excess, more = max(self.excess + k, 0), max(-k - self.excess, 0)
            self.ready.notify_all()
            return more

    def sojourn_ms(self) -> float:  # the mean time from due to commit: the wait at C, the queue and the service
        return self.sojourn_us / self.commits / 1e3 if self.commits else 0.0

    def report(self, name: str, total: Summary, last: bool) -> None:  # a newer report replaces the one unread
        with self.clock.lock:
            self.stats[name] = (name, total, last)
            self.lasts += last
            self.news.notify()

    def reports(self, timeout: float) -> list[Report]:
        with self.clock.lock:
            self.clock.wait(self.news, lambda: self.lasts, max(timeout, 0.0))
            out, self.lasts = list(self.stats.values()), 0
            self.stats.clear()
            return out

    def depth(self) -> int:
        return self.items.qsize()


class ProcQueue:  # processes: a multiprocessing.Queue of at most C, and a stats queue for the senders' reports
    def __init__(self, capacity: int) -> None:
        self.items, self.stats, self.excess = mp.Queue(capacity), mp.Queue(), mp.Value("i", 0)

    def put(self, msg: Message | None) -> None:
        self.items.put(msg)

    def get(self, name: str) -> Message | None:
        while True:  # a stop comes before any message; an idle sender looks for one every 10 ms.
            # @todo I could maybe make this value configurable, perhaps create a structure for param overloads?
            with self.excess.get_lock():
                if self.excess.value:
                    self.excess.value -= 1
                    return None
            try:
                return self.items.get(timeout=0.01)
            except queue.Empty:
                pass

    def done(self, name: str, msg: Message, s: Summary) -> bool:
        return True  # the sender's own Summary is the commit, and it reaches the monitor as a report

    def stop(self, k: int) -> int:  # as LocalQueue.stop, on a counter every worker process shares
        with self.excess.get_lock():
            e = self.excess.value
            self.excess.value = max(e + k, 0)
            return max(-k - e, 0)

    def report(self, name: str, total: Summary, last: bool) -> None:
        self.stats.put((name, total, last))

    def reports(self, timeout: float) -> list[Report]:
        out = []
        try:
            out.append(self.stats.get(timeout=max(timeout, 0.001)))
            while True:
                out.append(self.stats.get_nowait())
        except queue.Empty:
            return out

    def sojourn_ms(self) -> float:  # not measured: each worker process has its own clock
        return -1.0

    def depth(self) -> int:
        try:
            return self.items.qsize()
        except NotImplementedError:  # macOS has no sem_getvalue, bah
            return -1


# note, I had not initally modeled in a key rate in the depths my initial FP lark on this problem, seemed too dynamic
# that obviously defeats the purpose of a Producer process, if the producer is distilled into a pre-folded hashmap
# so now we have rates
def live_rate(key: str | None, last: float) -> float:  # the key rate, or the last rate if it is no number in bounds
    try:
        return r if fate.PARAMS["rate"]["min"] <= (r := float(key or 0)) <= fate.PARAMS["rate"]["max"] else last
    except ValueError:
        return last


class StreamQueue:  # pods: the stream "sms" and its consumer group "senders"; each sender is one consumer
    def __init__(self, r: Any, capacity: int, lease_ms: int = 30_000, clock: Clock | None = None) -> None:
        self.r, self.c, self.lease, self.clock = r, capacity, lease_ms, clock or Clock()
        self.held: dict[str, str] = {}  # sender name -> the stream id of the message in its hand
        self.room, self.rate, self.halt = 0, 0.0, False  # the producer's: puts left before it reads the depth again,
        # and the keys rate and halt, re-read with every put
        self.stopping = 0  # once the senders must stop, the pod's exit code: 143 on SIGTERM, 1 after an error
        self.commit = r.register_script(COMMIT)  # runs as EVALSHA
        try:
            r.xgroup_create("sms", "senders", "0", mkstream=True)
        except Exception as e:  # whoever starts first creates the group
            if "BUSYGROUP" not in str(e):
                raise

    def pending(self) -> int:  # delivered and not yet acked: message in flight
        return int(self.r.xpending("sms", "senders")["pending"])

    def depth(self) -> int:  # an acked entry is deleted, so the stream holds the queued and the in flight
        n, p = self.r.pipeline().xlen("sms").xpending("sms", "senders").execute()  # MULTI/EXEC: one snapshot
        return int(n) - int(p["pending"])

    def put(self, msg: Message | None) -> None:  # only the producer adds, so the depth is at most C - room
        while self.room <= 0:  # backpressure: poll every 10 ms
            self.room = self.c - self.depth()
            if self.room <= 0:
                self.clock.sleep(0.01)
        self.room -= 1
        both = self.r.pipeline(transaction=False)  # one round trip, which also reads the live rate and the halt
        both.xadd("sms", {"id": msg.id, "body": msg.body, "created_us": msg.created_us})
        both.incr("made")
        rate, halt = both.mget("rate", "halt").execute()[2]
        self.rate, self.halt = live_rate(rate, self.rate), halt == "1"

    def get(self, name: str) -> Message | None:
        while not self.stopping:
            got = self.r.xautoclaim("sms", "senders", name, self.lease, "0-0", count=1)[1]  # a lease ran out
            if not got:
                got = (self.r.xreadgroup("senders", name, {"sms": ">"}, count=1, block=1000) or [[None, []]])[0][1]
            if got:
                self.held[name], f = got[0]
                return Message(int(f["id"]), f["body"].encode(), int(f["created_us"]))
            if self.r.get("stop") == "1":
                return None
        return None

    def done(self, name: str, msg: Message, s: Summary) -> bool:
        return self.commit(keys=["sms", "totals"], args=[self.held.pop(name), *s]) == 1

    def renew(self) -> None:  # reset the idle time of every message in hand, so a live sender's lease never runs out
        for name, sid in self.held.copy().items():
            self.r.xclaim("sms", "senders", name, 0, [sid], justid=True)

    def report(self, name: str, total: Summary, last: bool) -> None:  # `sms consume` writes the snapshots every T
        if last:
            self.r.hset(f"snap:{name}", mapping=total._asdict())

    def watch(self, cfg: Config, emit: Callable[[Snap], None]) -> Summary:  # the pipeline's loop, without scaling
        t0, made0, done0, offered = 0, 0, 0, 0.0
        target = cfg.workers  # the pods the controller would run; this loop only prints it
        while True:  # every T: depth, pending, done, the rates and the target; then stop the senders at N
            t, pending, made, depth = self.clock.now(), self.pending(), int(self.r.get("made") or 0), self.depth()
            totals, (rate, halt) = self.r.hgetall("totals"), self.r.mget("rate", "halt")
            done, dt, offered = int(totals.get("done", 0)), max(t - t0, 1e-9), live_rate(rate, offered)
            tau = int(totals["time_us"]) / done / 1e6 if done else mean_tau_s(cfg)  # measured once there is a done
            # open arrivals: enough pods for the offered rate (Little's law: busy senders = rate E[tau]); and a full
            # queue means too few senders, so at least one pod more
            want = math.ceil(offered * tau / cfg.senders) if offered else target
            target = min(max(want, target + 1 if depth >= cfg.capacity else 1), P_MAX)
            emit(
                {
                    "t": t,
                    "depth": depth,
                    "pending": pending,
                    "done": done,
                    "lambda_offered": offered,
                    "lambda_in": (made - made0) / dt,
                    "lambda_out": (done - done0) / dt,
                    "target": target,
                }
            )
            if done >= cfg.n or halt == "1" or self.stopping:  # done, or halted by the demo or by Ctrl-C
                break
            t0, made0, done0 = t, made, done
            self.clock.sleep(cfg.interval_ms / 1e3)
        self.r.mset({"stop": 1, "halt": int(done < cfg.n)})  # the senders exit 0; a halt also ends the producer
        self.r.xtrim("sms", maxlen=0)
        return Summary(*(int(totals.get(f, 0)) for f in Summary._fields))


def mean_tau_s(cfg: Config) -> float:  # E[tau] from the config: attempt j runs with probability p^j, after its backoff
    p = cfg.fail_ppm / 1e6
    return sum(p**j * (cfg.mean_us + (cfg.backoff_us << (j - 1) if j else 0)) for j in range(cfg.attempts)) / 1e6


class Sender:  # the same blocking loop at every level where we get, send, commit, and report its own Summary
    def __init__(self, q: Queue, cfg: Config, clock: Clock, name: str) -> None:
        self.q, self.gateway, self.name, self.total = q, Gateway(cfg, clock.sleep), name, Summary()

    def run(self) -> None:
        while (msg := self.q.get(self.name)) is not None:
            s = self.gateway.send(msg)
            if self.q.done(self.name, msg, s):  # a message another sender committed first is not this one's
                self.total += s
            self.q.report(self.name, self.total, False)
        self.q.report(self.name, self.total, True)


class Producer:  # ids 0..N-1 as Messages, with arrivals at rate(i) msg/s; 0 is the closed batch
    def __init__(self, q: Queue, cfg: Config, rate: Callable[[int], float], clock: Clock) -> None:
        self.q, self.cfg, self.rate, self.clock, self.made, self.end = q, cfg, rate, clock, 0, 0.0

    def run(self) -> None:  # message i is due at the sum of the gaps before it, however long a put waited at C
        due, last = 0.0, None  # due is stamped as created_us: a latency from it counts the wait at C
        for i in range(self.cfg.n):
            self.q.put(Message(i, fate.body(self.cfg.seed, i), round(due * 1e6)))
            self.made += 1
            if (r := self.rate(i)) != last:  # the schedule restarts at now only at the start or after the closed batch
                due, last = (due if last else max(due, self.clock.now())), r
            due += fate.gap_s(self.cfg.seed, i, r)
            self.clock.sleep(max(due - self.clock.now(), 0.0))  # behind the schedule after a wait: no sleep
        self.end = self.clock.now()


class Monitor:  # every T: the sum of each sender's latest report; it ends when every sender ever started has stopped
    def __init__(self, pipe: Pipeline) -> None:
        self.pipe, self.latest, self.stopped, self.total = pipe, dict[str, Summary](), 0, Summary()

    def run(self, emit: Callable[[Snap], None]) -> None:
        p, k, period = self.pipe, 0, self.pipe.cfg.interval_ms / 1e3
        while True:
            for name, s, last in p.q.reports(k * period - p.clock.now()):
                self.latest[name], self.stopped = s, self.stopped + last
            end = p.closed and self.stopped == p.spawned
            if end or p.clock.now() >= k * period:
                self.total = sum(self.latest.values(), Summary())
                more = {"depth": p.q.depth(), "made": p.producer.made, "sojourn_ms": p.q.sojourn_ms()}
                emit(snap(p.clock.now(), self.total) | more)
                k += 1
            if end:
                return


def work(q: ProcQueue, cfg: Config, names: list[str]) -> None:  # one worker process: a sender thread per name
    clock = Clock()
    for t in [clock.spawn(Sender(q, cfg, clock, name).run) for name in names]:
        t.join()


class Pipeline:  # creates queue, starts the producer, start senders and start the monitor, scale(m) while it runs
    def __init__(self, cfg: Config, level: str = "thread", rates: Sequence[int] = (), clock: Clock | None = None):
        self.cfg, self.level, self.clock = cfg, level, clock or Clock()
        self.q = LocalQueue(self.clock, cfg.capacity) if level == "thread" else ProcQueue(cfg.capacity)
        rates = rates or (cfg.rate,)  # a schedule a,b,c: each rate for a third of the ids
        self.producer = Producer(self.q, cfg, lambda i: rates[i * len(rates) // cfg.n], self.clock)
        self.monitor, self.procs = Monitor(self), list[mp.Process]()
        self.spawned = self.live = 0
        self.closed = False

    def spawn(self, k: int) -> None:  # k more senders: k threads, or one process running k sender threads
        names = [f"s{j}" for j in range(self.spawned, self.spawned + k)]
        self.spawned += k
        if self.level == "thread":
            for name in names:
                self.clock.spawn(Sender(self.q, self.cfg, self.clock, name).run)
        else:
            self.procs.append(p := mp.Process(target=work, args=(self.q, self.cfg, names)))
            p.start()

    def scale(self, m: int) -> None:  # shrink: the next senders to get stop; grow: cancel those stops, then spawn
        with self.clock.lock:
            if self.closed:
                return
            m = max(m, 1)
            if more := self.q.stop(self.live - m):
                self.spawn(more)
            self.live = m

    def run(self, emit: Callable[[Snap], None], plan: Sequence[tuple[float, int]] = ()) -> Summary:
        # plan: (after s, M) steps for a controller; the committed total is the sum of every sender's last report
        self.clock.spawn(self.root, emit, plan).join()
        self.watch.join()
        for p in self.procs:
            p.join()
        return self.monitor.total

    def root(self, emit: Callable[[Snap], None], plan: Sequence[tuple[float, int]]) -> None:  # starts every thread
        self.watch = self.clock.spawn(self.monitor.run, emit)
        for _ in range(self.cfg.workers if self.level == "process" else 1):
            self.scale(self.live + self.cfg.senders)
        if plan:
            self.clock.spawn(self.control, plan)
        self.producer.run()
        with self.clock.lock:  # the producer is done: senders drain what is queued, then stop
            self.closed = True
            for _ in range(self.live):
                self.q.put(None)

    def control(self, plan: Sequence[tuple[float, int]]) -> None:
        for after, m in plan:
            self.clock.sleep(after)
            self.scale(m)


def snap(t: float, s: Summary) -> Snap:
    n = s.sent + s.failed
    return {"t": t, **s._asdict(), "mean_ms": s.time_us / n / 1e3 if n else 0.0, "rate": n / t if t else 0.0}


def print_snap(x: Snap) -> None:  # t=1.0 sent=101 failed=11 attempts=112 time_us=9050112 mean_ms=80.8 rate=111.92
    print(" ".join(f"{k}={round(v, 2)}" for k, v in x.items()), file=sys.stderr, flush=True)
