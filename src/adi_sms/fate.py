"""Every random choice is a pure function of (seed, message i, attempt j, purpose)"""

import functools
import json
import math
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, fields
from pathlib import Path
from typing import NamedTuple

# spec.json is rendered from spec/Spec.dhall; frozendict (new in 3.15) makes every object in it immutable
SPEC = json.loads((Path(__file__).parent / "spec.json").read_text(), object_hook=frozendict)
PARAMS, MAX_LEN, ALPHABET = SPEC["params"], SPEC["max_len"], SPEC["alphabet"].encode()
MASK, GOLDEN = (1 << 64) - 1, 0x9E3779B97F4A7C15
LEN, BODY, LAT, FAIL, ARR = 1, 2, 3, 4, 5  # purpose tags keep the draws apart; renumbering changes every fate
TABLE = bytes(ALPHABET[b & 63] for b in range(256))  # byte -> symbol; 64 divides 256, so unbiased


# Hey there! Before digging into this poorly documented file, I suggest reading the docs
# at https://adi.clients.xoxd.ai/docs
# to understand how I've ended up with the following functions and structure.
@dataclass(frozen=True, slots=True)
class Config:
    n: int  # N messages
    seed: int  # s
    senders: int  # M concurrent senders
    capacity: int  # C: queue bound (backpressure)
    mean_us: int  # mu: mean attempt latency
    fail_ppm: int  # p: failure probability per attempt, parts per million
    attempts: int  # A: at most A attempts per message
    backoff_us: int  # beta: wait beta 2^(j-1) before attempt j >= 1
    interval_ms: int  # T: monitor period
    rate: int  # lambda: arrivals per second, 0 = all at once (the closed batch)
    workers: int  # P: processes (or pods), each running M senders

    def __post_init__(self) -> None:  # every construction and replace() is bounds-checked
        for f in fields(self):
            v, p = getattr(self, f.name), PARAMS[f.name]
            if type(v) is not int or not p["min"] <= v <= p["max"]:
                raise ValueError(f"{f.name}={v!r}: want an int in [{p['min']}, {p['max']}]")


DEFAULTS = Config(**{k: p["default"] for k, p in PARAMS.items()})  # TypeError if spec and code drift


class Summary(NamedTuple):  # a commutative monoid under componentwise + with identity Summary()
    sent: int = 0
    failed: int = 0
    attempts: int = 0
    time_us: int = 0  # service time: every attempt's latency plus backoff, without queue wait

    def __add__(self, o: tuple[int, ...]) -> Summary:
        return Summary(self[0] + o[0], self[1] + o[1], self[2] + o[2], self[3] + o[3])


def mix(z: int) -> int:  # SplitMix64's finalizer: a bijection on 64-bit words that spreads every bit
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
    return z ^ (z >> 31)


def absorb(h: int, w: int) -> int:
    return mix(((h ^ w) + GOLDEN) & MASK)


@functools.cache  # the seed's round is the same for every id, so it runs once per seed
def root(seed: int) -> int:
    return absorb(0, seed)


def key(seed: int, i: int) -> int:  # the seed and the id are absorbed in separate rounds
    return absorb(root(seed), i)


def draw(k: int, purpose: int, j: int) -> int:
    return absorb(k, (purpose << 56) | j)


def length(k: int) -> int:
    return draw(k, LEN, 0) % (MAX_LEN + 1)  # U{0..100}


def body(seed: int, i: int) -> bytes:
    k = key(seed, i)
    n = length(k)
    raw = b"".join(draw(k, BODY, b).to_bytes(8, "little") for b in range((n + 7) // 8))
    return raw[:n].translate(TABLE)


def latency_us(k: int, j: int, mean_us: int) -> int:
    u = (draw(k, LAT, j) >> 11) * 2.0**-53
    return math.floor(-mean_us * math.log1p(-u))


def fails(k: int, j: int, fail_ppm: int) -> bool:
    return draw(k, FAIL, j) < (fail_ppm << 64) // 1_000_000  # an exact integer Bernoulli(p)


def deliver(c: Config, i: int) -> tuple[list[int], Summary]:
    # message i's whole fate before it starts: each attempt's wait (us), then the outcome
    k, waits = key(c.seed, i), []
    for j in range(c.attempts):
        waits.append((c.backoff_us << (j - 1) if j else 0) + latency_us(k, j, c.mean_us))
        if not fails(k, j, c.fail_ppm):
            return waits, Summary(1, 0, len(waits), sum(waits))
    return waits, Summary(0, 1, len(waits), sum(waits))


def gap_s(seed: int, i: int, rate: float) -> float:  # the wait from message i to i+1: Exp(1 / rate) seconds, keyed
    u = (draw(key(seed, i), ARR, 0) >> 11) * 2.0**-53
    return -math.log1p(-u) / rate if rate > 0 else 0.0


@dataclass(frozen=True, slots=True)
class Message:
    id: int
    body: bytes
    created_us: int  # us since the producer started


class Gateway:  # the far side: a send takes the fate's waits and returns the fate's outcome
    def __init__(self, c: Config, sleep: Callable[[float], None] = time.sleep) -> None:
        self.c, self.sleep = c, sleep

    def send(self, msg: Message) -> Summary:
        waits, s = deliver(self.c, msg.id)
        for w in waits:  # each attempt's backoff plus latency: a retry stays with its sender
            self.sleep(w / 1e6)
        return s


def run_range(c: Config, a: int, b: int, threads: int = 1) -> Summary:  # the pure fold over ids [a, b)
    if threads <= 1:
        return sum((deliver(c, i)[1] for i in range(a, b)), Summary())
    cuts = [a + t * (b - a) // threads for t in range(threads + 1)]  # chunk t: [cuts[t], cuts[t + 1]), one thread each
    with ThreadPoolExecutor(threads) as pool:
        return sum(pool.map(run_range, [c] * threads, cuts, cuts[1:]), Summary())
