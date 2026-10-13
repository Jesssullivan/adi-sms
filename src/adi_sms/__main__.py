"""sms run | produce | consume | monitor | fold. Every Config field is a flag; run's --rate also takes a,b,c."""

import argparse
import json
import signal
import sys

from .fate import PARAMS, Config, Summary, run_range
from .pipeline import Pipeline, Producer, Sender, StreamQueue, print_snap, snap


def dump(d: dict[str, object]) -> str:
    return json.dumps(d, separators=(",", ":"))


def decimal(s: str) -> int:  # ASCII digits only, as the ports parse: no "1_000", "+1", " 1" or other scripts' digits
    if not (s.isascii() and s.isdigit()):
        raise ValueError(f"{s!r}: want ASCII decimal digits")
    return int(s)


def client(addr: str):  # HOST:PORT, db 0, no auth
    import redis  # only the pod level needs it

    host, port = addr.rsplit(":", 1)
    return redis.Redis(host, int(port), decode_responses=True)


def final(cfg: Config, s: Summary) -> None:  # the Summary, whether it equals the pure fold, and exit 1 if not
    ok = s == run_range(cfg, 0, cfg.n)
    print(dump({**s._asdict(), "check": ok}))
    sys.exit(0 if ok else 1)


def consume(cfg: Config, addr: str, name: str, lease_ms: int) -> None:  # M sender threads on the stream until stop
    q = StreamQueue(client(addr), cfg.capacity, lease_ms)
    signal.signal(signal.SIGTERM, lambda *_: setattr(q, "stopping", q.stopping or 143))  # 128 + SIGTERM: scaled down
    senders = [Sender(q, cfg, q.clock, f"{name}-{k}") for k in range(cfg.senders)]
    threads = [q.clock.spawn(s.run) for s in senders]
    while any(t.is_alive() for t in threads):  # every T, at most a third of the lease: renew, snapshot, this pod's line
        if not all(t.is_alive() for t in threads):  # a sender ends only at stop, at SIGTERM or on an error
            q.stopping = q.stopping or 1
        q.renew()
        for s in senders:
            q.r.hset(f"snap:{s.name}", mapping=s.total._asdict())
        print_snap(snap(q.clock.now(), sum((s.total for s in senders), Summary())))
        end = q.clock.now() + min(cfg.interval_ms, lease_ms / 3) / 1e3
        for t in threads:
            t.join(max(0.0, end - q.clock.now()))
    total = sum((s.total for s in senders), Summary())
    print_snap(snap(q.clock.now(), total))
    print(dump(total._asdict()))
    sys.exit(0 if q.r.get("stop") == "1" else q.stopping or 1)  # 143 after SIGTERM, 1 after a sender's error


def main() -> None:
    ap = argparse.ArgumentParser(prog="sms", description=__doc__, allow_abbrev=False)
    sub = ap.add_subparsers(dest="mode", required=True)
    run = sub.add_parser("run", help="one run on this machine: monitor lines to stderr, the Summary to stdout")
    run.add_argument("--level", choices=["thread", "process"], default="thread", help="M threads, or P processes x M")
    fold = sub.add_parser("fold", help="the pure oracle: the Summary of all N messages, without running")
    fold.add_argument("--threads", type=decimal, default=1, help="split the fold over T threads")
    prod = sub.add_parser("produce", help="put N messages on the stream at the live rate")
    cons = sub.add_parser("consume", help="run M senders on the stream until stop")
    cons.add_argument("--name", default="sms", help="the senders are consumers NAME-0 .. NAME-(M-1)")
    cons.add_argument("--lease-ms", type=decimal, default=30_000, help="reclaim a message pending this long")
    mon = sub.add_parser("monitor", help="every T print the stream's depth and rates; at N or Ctrl-C stop the run")
    for p in (prod, cons, mon):
        p.add_argument("--queue", required=True, metavar="HOST:PORT", help="the Valkey server")
    for p in (run, fold, prod, cons, mon):
        p.allow_abbrev = False  # a subparser does not inherit it: --mean-us, never --mean
        for name, v in PARAMS.items():
            doc = f"{v['doc']} (default {v['default']}, {v['min']}..{v['max']})"
            p.add_argument(
                f"--{name.replace('_', '-')}",
                type=str if name == "rate" else decimal,
                default=v["default"],
                help=doc + ("; run takes a,b,c: one rate per third of the ids" if name == "rate" else ""),
            )
    a = ap.parse_args()
    try:
        rates = [decimal(r) for r in str(a.rate).split(",")]
        if len(rates) > 1 and a.mode != "run":
            raise ValueError("--rate a,b,c: only sms run takes a schedule")
        cfgs = [Config(**{name: getattr(a, name) for name in PARAMS} | {"rate": r}) for r in rates]  # each in bounds
    except ValueError as e:
        ap.error(str(e))
    cfg = cfgs[0]
    if a.mode == "fold":
        print(dump(run_range(cfg, 0, cfg.n, a.threads)._asdict()))
    elif a.mode == "run":
        final(cfg, Pipeline(cfg, a.level, rates).run(print_snap))
    elif a.mode == "produce":  # the rate key may be set already (the demo's slider); each put re-reads it
        q = StreamQueue(client(a.queue), cfg.capacity)
        q.r.set("n", cfg.n)
        q.r.setnx("rate", cfg.rate)
        Producer(q, cfg, lambda i: sys.exit(0) if q.halt else q.rate, q.clock).run()  # halt "1": stop, exit 0
        q.r.set("produced", cfg.n)
    elif a.mode == "consume":
        consume(cfg, a.queue, a.name, a.lease_ms)
    else:  # Ctrl-C halts the run, as the demo's halt button does: SET halt 1 and SET stop 1 at the next tick
        q = StreamQueue(client(a.queue), cfg.capacity)
        signal.signal(signal.SIGINT, lambda *_: setattr(q, "stopping", 130))
        final(cfg, q.watch(cfg, print_snap))


if __name__ == "__main__":
    main()
