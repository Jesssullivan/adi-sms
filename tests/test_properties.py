# Each property name are frozen in spec/Spec.dhall. Each test's docstring maps something important from the brief,
# the sms_simulation.md file.

import contextlib
import gc
import itertools
import json
import math
import os
import shutil
import socket
import subprocess
import sys
import threading
import tracemalloc
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from hypothesis import example, given, settings
from hypothesis import strategies as st
from redis import Redis
from redis.backoff import ConstantBackoff
from redis.retry import Retry

from adi_sms import fate
from adi_sms.fate import ALPHABET, DEFAULTS, MAX_LEN, Config, Summary, deliver, run_range
from adi_sms.pipeline import Clock, Pipeline, Producer, Sender, Snap, StreamQueue, live_rate, mean_tau_s

settings.register_profile("ci", max_examples=60, deadline=None, derandomize=True, database=None)
settings.register_profile("deep", max_examples=1_000, deadline=None)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "ci"))
ROOT = Path(__file__).parents[1]
V = json.loads((ROOT / "spec/generated/vectors.json").read_text())
assert V["schema"] == "adi-sms-vectors-v2", V["schema"]
TOL = 0 if sys.platform == "linux" else 1  # off Linux, libm's log1p may differ by 1 ulp: allow 1 us per attempt
U64, U53 = st.integers(0, 2**64 - 1), st.integers(0, 2**53 - 1)

# small runs that vary every field; the vectors, the examples and calibrated_fate reach the spec's full ranges
CONFIGS = st.builds(
    replace,
    st.just(DEFAULTS),
    n=st.integers(0, 400),
    seed=U53,
    senders=st.integers(1, 24),
    capacity=st.integers(1, 48),
    mean_us=st.integers(0, 50_000),
    fail_ppm=st.integers(0, 10**6),
    attempts=st.integers(1, 5),
    backoff_us=st.integers(0, 20_000),
    interval_ms=st.integers(100, 2_000),
)
# a controller's moves: after this many ms, scale to M senders
PLANS = st.lists(st.tuples(st.integers(0, 2_000), st.integers(1, 24)), max_size=4)


def fold(cfg: Config) -> Summary:  # the pure fold over ids [0, N): the total every run must commit
    return run_range(cfg, 0, cfg.n)


class Everyone(threading.Condition):  # in virtual time every wake-up is a broadcast, so no waiter is missed
    def notify(self, n: int = 1) -> None:
        super().notify(1 << 30)


class VirtualClock(Clock):  # when every live thread waits, time jumps to the earliest deadline
    def __init__(self, limit: float) -> None:
        super().__init__()
        self.cv, self.t, self.limit, self.live, self.waiting, self.dead = Everyone(self.lock), 0.0, limit, 0, [], False

    def now(self) -> float:
        return self.t

    def condition(self) -> threading.Condition:
        return self.cv

    def sleep(self, d: float) -> None:
        with self.lock:
            self.wait(self.cv, lambda: False, d)

    def spawn(self, f, *args) -> threading.Thread:
        def body() -> None:
            try:
                f(*args)
            finally:
                with self.lock:
                    self.live -= 1
                    self.cv.notify_all()

        with self.lock:
            self.live += 1
        return super().spawn(body)

    def wait(self, cv, ready, timeout=None) -> bool:  # with the lock held
        me = [ready, math.inf if timeout is None else self.t + timeout]
        self.waiting.append(me)
        try:
            while not ready() and self.t < me[1]:
                if self.dead:
                    raise RuntimeError("virtual-time deadlock")
                if len(self.waiting) == self.live and not any(r() or self.t >= u for r, u in self.waiting):
                    self.t = min(u for _, u in self.waiting)  # nobody can run before then
                    self.dead = self.t > self.limit  # past any honest finish, or nothing will ever wake
                    self.cv.notify_all()
                else:
                    self.cv.wait()
        finally:
            self.waiting.remove(me)
        return bool(ready())


# One run in virtual time. The monitor always waits with a deadline, so a stuck run shows as time past its limit:
# the total service time S (Graham's bound at M = 1), the controller's moves and the arrivals.
def vrun(cfg: Config, plan=(), rates=()) -> tuple[Pipeline, list[Snap]]:
    span = sum(fate.gap_s(cfg.seed, i, r) for r in rates or (cfg.rate,) for i in range(cfg.n))
    clock = VirtualClock(fold(cfg).time_us / 1e6 + sum(a for a, _ in plan) / 1e3 + span + 1)
    pipe, snaps = Pipeline(cfg, "thread", rates, clock), list[Snap]()
    pipe.run(snaps.append, [(a / 1e3, m) for a, m in plan])
    assert not clock.dead
    return pipe, snaps


@given(seed=U64, i=U64, want=st.none())
def test_message_valid(seed: int, i: int, want: dict | None) -> None:
    """Brief: "Each message contains up to 100 random characters."
    Property: for all (seed, i), body(seed, i) is 0 to 100 symbols of the alphabet and the same on every call.
    Asserts: the length, the alphabet and a second call; on each golden vector, every fate output bit for bit.
    Catches: a body over 100 or off the alphabet, hidden state, drift from what Fate.hs and the ports replay."""
    k, body = fate.key(seed, i), fate.body(seed, i)
    assert len(body) == fate.length(k) <= MAX_LEN and set(body) <= set(ALPHABET) and body == fate.body(seed, i)
    if want:  # the mixer, the key, the length, the text, the ARR draw, three outcomes and latencies
        assert all(fate.mix(m["in"]) == m["out"] for m in V["mix"])
        assert (k, len(body), body.decode()) == (want["key"], want["len"], want["body"])
        assert [fate.fails(k, j, V["fail_ppm"]) for j in range(3)] == want["fail"]
        assert fate.draw(k, fate.ARR, 0) == want["arr"]  # the raw 64-bit word that gap_s turns into a wait
        assert all(abs(fate.latency_us(k, j, V["mean_us"]) - want["lat"][j]) <= TOL for j in range(3))


for c, a in zip(V["cases"], V["arr"], strict=True):  # every golden case in spec/generated/vectors.json is an example
    test_message_valid = example(seed=c["seed"], i=c["i"], want=c | {"arr": a})(test_message_valid)


@contextlib.contextmanager
def valkey(argv: list[str], host: str = "127.0.0.1"):  # argv's server on a free port of host, and a client of it
    if not shutil.which(argv[0]):
        pytest.fail("the stream tests need valkey-server; run inside `nix develop`")
    with socket.socket() as f:
        f.bind((host, 0))
        argv[argv.index("--port") + 1] = str(port := f.getsockname()[1])
    server = subprocess.Popen(argv, stdout=subprocess.DEVNULL)
    try:  # the client retries every 50 ms while the server starts
        with Redis(host, port, decode_responses=True, retry=Retry(ConstantBackoff(0.05), 100)) as client:
            yield client
    finally:
        server.terminate()
        server.wait()


def pods(cfg: Config) -> None:  # the pod level: senders on a Valkey stream, one consumer killed holding a message
    job = yaml.safe_load((ROOT / "deploy/queue.yaml").read_text())  # the template loads as it is: ${...} are scalars
    host = "127.0.0.1"  # a build sandbox has no address but loopback
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s, contextlib.suppress(OSError):
        s.connect(("192.0.2.1", 9))  # UDP sends nothing: the kernel only picks this host's outgoing address
        host = s.getsockname()[0]
    with valkey(job["spec"]["template"]["spec"]["containers"][0]["command"], host) as r:  # by a pod IP
        assert r.ping()  # protected mode would answer DENIED to every non-loopback client
        q = StreamQueue(r, cfg.capacity, lease_ms=50)
        producer = q.clock.spawn(Producer(q, cfg, lambda i: 0.0, q.clock).run)
        # it dies holding a message: the entry moves after the lease
        lost, stale = q.get("dead-0"), q.held.pop("dead-0")
        senders = [Sender(q, cfg, q.clock, f"live-{k}") for k in range(cfg.senders)]
        threads = [q.clock.spawn(s.run) for s in senders]
        snaps = list[Snap]()
        total = q.watch(cfg, snaps.append)  # at done == N it sets stop, and the senders exit
        for t in [producer, *threads]:
            t.join()
        q.held["dead-0"] = stale
        assert not q.done("dead-0", lost, deliver(cfg, lost.id)[1])  # the dead consumer's late commit is not new
        assert total == sum((s.total for s in senders), Summary()) == fold(cfg)  # a stolen send counts once
        assert q.r.hgetall("totals") == {**{f: str(v) for f, v in total._asdict().items()}, "done": str(cfg.n)}
        assert q.depth() == q.pending() == 0 and max(s["depth"] for s in snaps) <= cfg.capacity
        assert max(s["pending"] for s in snaps) <= cfg.senders + 1  # in flight: the live senders' and the dead one's


POD = replace(DEFAULTS, n=400, senders=4, capacity=20, mean_us=200, fail_ppm=200_000, interval_ms=100)
PROCESS = replace(DEFAULTS, n=300, senders=3, workers=2, mean_us=300, interval_ms=100)  # 2 workers of 3 threads


# the default run, scaled down and up
@example(cfg=DEFAULTS, plan=[(500, 1), (500, 24), (0, 3)], a=0, want=None, level="thread")
@example(cfg=PROCESS, plan=[(20, 9), (20, 1), (10, 4)], a=0, want=None, level="process")  # scaled to 9, 1 and 4
@example(cfg=POD, plan=[], a=0, want=None, level="pod")
# 20 ms against the 50 ms lease: about 8% of sends are stolen
@example(cfg=replace(POD, mean_us=20_000), plan=[], a=0, want=None, level="pod")
@given(cfg=CONFIGS, plan=PLANS, a=st.integers(0, 2**64 - 401), want=st.none(), level=st.just("thread"))
def test_fold_equivalence(cfg: Config, plan: list[tuple[int, int]], a: int, want: list[int] | None, level: str) -> None:
    """Brief: "Configurable number of senders. Each sender: Consumes messages from the producer."
    Property: for all configs and controller plans, the committed total equals the fold, whatever M does mid-run.
    Asserts: the total is the fold for threads, processes and pods (Valkey); any id range split over 3 threads too.
    Catches: the model's DoubleTake and DoubleCommit, a dropped or doubled message, protected mode, a non-monoid sum."""
    whole = run_range(cfg, a, a + cfg.n)
    assert run_range(cfg, a, a + cfg.n, threads=3) == whole
    assert want is None or whole[:3] == tuple(want[:3]) and abs(whole.time_us - want[3]) <= TOL * whole.attempts
    if level == "thread":
        assert vrun(cfg, plan)[0].monitor.total == fold(cfg)
    if level == "process":  # P processes of M sender threads on one multiprocessing.Queue
        assert Pipeline(cfg, "process").run(lambda _: None, [(t / 1e3, m) for t, m in plan]) == fold(cfg)
    if level == "pod":
        pods(cfg)


for r in V["runs"]:  # every golden run, the default known answer first, is an explicit example
    cfg = replace(DEFAULTS, **r["cfg"], n=r["b"] - r["a"])
    test_fold_equivalence = example(cfg=cfg, plan=[], a=r["a"], want=r["summary"], level="thread")(
        test_fold_equivalence
    )


DELTA, N = 1e-9, 1 << 16  # false-alarm probability per statistical assertion; messages per check
L = math.log(2 / DELTA)


def tolerance(q: float) -> float:  # Bernstein's bound on |mean - q| for N Bernoulli(q) draws, capped by Hoeffding's
    return min(math.sqrt(L / (2 * N)), math.sqrt(2 * q * (1 - q) * L / N) + 2 * L / (3 * N))


def chernoff() -> float:  # the smallest e with exp(-N (e - ln(1 + e))) <= DELTA / 2, by bisection
    lo, hi = 0.0, 1.0  # the upper tail of a mean of N exponentials; the lower tail is tighter
    for _ in range(60):
        mid = (lo + hi) / 2
        lo, hi = (mid, hi) if N * (mid - math.log1p(mid)) < L else (lo, mid)
    return hi


@settings(max_examples=5)
@given(seed=U53, ppm=st.integers(0, 10**6), mean_us=st.integers(0, 10**7), a=st.integers(1, 10))
@example(seed=1, ppm=0, mean_us=0, a=1)
@example(seed=1, ppm=1_000_000, mean_us=1, a=3)
def test_calibrated_fate(seed: int, ppm: int, mean_us: int, a: int) -> None:
    """Brief: "Simulates sending by waiting a random time (configurable mean). Has a configurable failure rate."
    Property: for all seeds, p, mu and A, over 2^16 messages the rates are near p and p^A and the mean latency near mu.
    Asserts: Bernstein bounds on the rates, a Chernoff bound on the mean at delta = 1e-9; p = 0 and p = 1 exactly.
    Catches: a biased draw, Exp(1 / mu) where Exp(mu) is meant, a wrong backoff or E[tau], gaps off 1 / lambda."""
    keys, p = [fate.key(seed, i) for i in range(N)], ppm / 1e6
    assert abs(sum(fate.fails(k, 0, ppm) for k in keys) / N - p) <= tolerance(p)  # per attempt
    terminal = sum(all(fate.fails(k, j, ppm) for j in range(a)) for k in keys) / N
    assert abs(terminal - p**a) <= tolerance(p**a)  # P(failed) = p^A
    mean = sum(fate.latency_us(k, 0, mean_us) for k in keys) / N
    assert abs(mean - mean_us) <= chernoff() * mean_us + 1  # Exp(mu); +1 us: latencies are floored
    c = replace(DEFAULTS, seed=seed, mean_us=mean_us, attempts=a, backoff_us=10_000)
    lat, backoff = [fate.latency_us(keys[0], j, mean_us) for j in range(a)], 10_000 * (2 ** (a - 1) - 1)
    assert deliver(replace(c, fail_ppm=0), 0)[1] == Summary(1, 0, 1, lat[0])
    assert deliver(replace(c, fail_ppm=10**6), 0)[1] == Summary(0, 1, a, backoff + sum(lat))  # every attempt fails
    assert math.isclose(mean_tau_s(replace(c, fail_ppm=10**6)), (backoff + a * mean_us) / 1e6)
    gaps = [fate.gap_s(seed, i, 1.0) for i in range(N)]  # arrivals: Exp(1 / lambda) gaps, keyed by the id
    assert abs(sum(gaps) / N - 1) <= chernoff() and gaps == [fate.gap_s(seed, i, 1.0) for i in range(N)]


def peak(cfg: Config) -> int:  # the run's heap peak in bytes
    gc.collect()
    tracemalloc.start()
    vrun(cfg)
    size = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    return size


@example(cfg=replace(DEFAULTS, n=500, senders=8, capacity=8, mean_us=1_000), plan=[], audit=True)
@example(cfg=replace(DEFAULTS, n=400, senders=24, capacity=48, mean_us=50_000), plan=[(100, 3), (0, 24)], audit=False)
@given(cfg=CONFIGS, plan=PLANS, audit=st.just(False))
def test_bounded_memory(cfg: Config, plan: list[tuple[int, int]], audit: bool) -> None:
    """Brief: "Generate a configurable number (default 1000)", and the spec lets N reach 10^9.
    Property: for all configs and plans, at most C are queued and at most max M in flight, so the heap is O(C + M).
    Asserts: the peaks, an empty queue at the end; in the audit, 16 times the messages within 32 KiB of the heap peak.
    Catches: the model's UnboundedQueue (the producer ignores C), senders added instead of reused on a scale-up."""
    pipe, _ = vrun(cfg, plan)  # a scale-down then up: the senders still stopping are reused, not added to
    assert pipe.q.peak_queued <= cfg.capacity and pipe.q.peak_inflight <= max([cfg.senders, *(m for _, m in plan)])
    assert pipe.q.depth() == pipe.q.inflight == 0
    if audit:
        assert peak(replace(cfg, n=16 * cfg.n)) - peak(cfg) < 32 * 1024


@given(cfg=CONFIGS)
@example(cfg=replace(DEFAULTS, n=2, senders=1, capacity=1, attempts=2, fail_ppm=10**6))
def test_throughput_law(cfg: Config) -> None:
    """Brief: "Configurable number of senders": M senders should finish about M times sooner.
    Property: for every closed batch, max(S/M, max tau) <= makespan <= S/M + (1 - 1/M) max tau (Graham, 1966).
    Asserts: both bounds in virtual time, where tau_i is message i's service time and S their sum.
    Catches: the model's RequeueRetry (it deadlocks in the example's shape), a sender idle while work is queued."""
    tau = [deliver(cfg, i)[1].time_us for i in range(cfg.n)] or [0]  # a sender is busy for all of tau_i
    m, total, longest = cfg.senders, sum(tau), max(tau)
    makespan = vrun(cfg)[1][-1]["t"] * 1e6  # the last snapshot is taken when the last sender stops
    assert max(total / m, longest) - 1 <= makespan <= total / m + (1 - 1 / m) * longest + 1  # 1 us: the float clock


@given(cfg=st.builds(replace, CONFIGS, mean_us=st.integers(0, 10**7)), plan=PLANS)  # mu's whole range: many periods
def test_monitor_monotone(cfg: Config, plan: list[tuple[int, int]]) -> None:
    """Brief: "Displays and updates (every N seconds, configurable)" the counts sent and failed.
    Property: for all configs and plans, snapshots start at 0, never decrease, come every T and end at the total.
    Asserts: t and every count never decrease; each gap is T, the last at most T; the last snapshot is the fold.
    Catches: a late or skipped tick, a count that goes down, a monitor that misses a sender's last report."""
    pipe, snaps = vrun(cfg, plan)
    keys = ("t", "made", *Summary._fields)
    assert snaps[0]["t"] == 0 and all(b[k] >= a[k] for a, b in itertools.pairwise(snaps) for k in keys)
    gaps, period = [b["t"] - a["t"] for a, b in itertools.pairwise(snaps)], cfg.interval_ms / 1e3
    assert all(abs(g - period) < 1e-6 for g in gaps[:-1]) and all(g <= period + 1e-6 for g in gaps[-1:])
    assert Summary(*(int(snaps[-1][k]) for k in Summary._fields)) == pipe.monitor.total == fold(cfg)


@settings(max_examples=10)
@given(seed=U53, m=st.integers(1, 8), load=st.sampled_from([0.5, 4.0]))
def test_little_law(seed: int, m: int, load: float) -> None:
    """Brief: "Scalability, concurrency, and performance" (the evaluation criteria).
    Property: for all seeds and M, at lambda = load * M / E[tau] the rates in and out follow min(lambda, capacity).
    Asserts: at half the capacity both within 15% of lambda; at 4 times, a full queue and both within 15% of capacity.
    Catches: a producer that ignores backpressure, and a sojourn stamped at the put (coordinated omission)."""
    keys = (None, "50", "x", "nan", "-1", "1000001", "1e9")  # the demo's rate key: a number in bounds, else the last
    assert [live_rate(k, 7.0) for k in keys] == [0.0, 50.0] + [7.0] * 5  # missing is 0
    cfg = replace(DEFAULTS, n=1_000, seed=seed, senders=m, capacity=20, mean_us=10_000, fail_ppm=0)
    capacity = m * 1e6 * cfg.n / fold(cfg).time_us  # M / E[tau], in messages per second
    cfg = replace(cfg, rate=max(1, round(load * capacity)))
    pipe, snaps = vrun(cfg)
    rate_in, rate_out = cfg.n / pipe.producer.end, cfg.n / snaps[-1]["t"]  # over the producer's run, and the whole
    want = cfg.rate if load < 1 else capacity  # stable: out keeps up with in; overloaded: both at the senders' pace
    assert abs(rate_in - want) <= 0.15 * want and abs(rate_out - want) <= 0.15 * want
    if load >= 1:  # stamped when due, the sojourn counts the slip behind the schedule: about 3/8 N / capacity
        assert pipe.q.peak_queued == cfg.capacity and pipe.q.sojourn_ms() >= 250 * cfg.n / capacity
