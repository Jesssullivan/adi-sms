-- adi-sms specification.  Welcome!
-- `just spec write` renders it to src/adi_sms/spec.json, which Python and the Haskell model read.
-- Each assert is checked whenever this file type-checks, so a spec that breaks one never renders.
-- Integer units only (us, ms, ppm), so every total is exact.
-- How to read this codebase from Spec.dhall's perspective:
-- ... This file -> rendered src/adi_sms/spec.json -> Python fate.py and model/Model.hs.
-- Python reads them as params (see frozendict). Dhall expressions are immutable values too.
-- The sms_simulation brief is indeed, quite brief, leaving a fair bit up to my imagination.
-- we are expected to have configurable messages, senders, wait mean, failure rate and reporting period.
-- All this other stuff is inferred, perhaps (probably? overthinking things here) needlessly.
-- Concepts such as seed, queue capacity, retries, the arrival rate and the worker count are choices
-- that I think make sense in this context. 
-- Param is a type;: each parameter record needs three nonnegative integers and explanatory text.
-- Entry and inBounds use this type to keep every parameter record in the same shape.
-- DHall is quite niche, I encurage you to explore Dhall as a specification language 
-- https://github.com/dhall-lang/dhall-lang 
let Param = { default : Natural, min : Natural, max : Natural, doc : Text }

let Entry = { mapKey : Text, mapValue : Param }
let le = \(a : Natural) -> \(b : Natural) -> Natural/isZero (Natural/subtract b a)
let inBounds = \(p : Param) -> le p.min p.default && le p.default p.max
let params =
      -- Brief: "configurable number (default 1000)" -> producer ID range [0,N).
      { n = { default = 1000, min = 0, max = 1000000000, doc = "N: messages" }
      -- Repeatable randomness -> Fate.key(seed, ID); fold_equivalence needs schedule-independent outcomes.
      , seed = { default = 1, min = 0, max = 9007199254740991, doc = "s: every random choice derives from it" }
      -- Brief: "Configurable number of senders" -> M in Python Pipeline and the Haskell pipeline shape.
      , senders = { default = 10, min = 1, max = 10000, doc = "M: concurrent senders" }
      -- Brief: "message passing" / "scalability" -> bounded queue; tested by bounded_memory.
      , capacity = { default = 10, min = 1, max = 100000, doc = "C: queue bound" }
      -- Brief: "random time (configurable mean)" -> Fate.latencyUs; Python calibrated_fate checks its mean.
      , mean_us = { default = 100000, min = 0, max = 10000000, doc = "mu: mean attempt latency, us" }
      -- Brief: "configurable failure rate" -> Fate.fails; ppm / 1000000 is probability per attempt.
      , fail_ppm = { default = 100000, min = 0, max = 1000000, doc = "p: failure probability per attempt, ppm" }
      -- Retry extension: total attempt limit A; Fate.deliver stops on success or after A attempts.
      , attempts = { default = 1, min = 1, max = 10, doc = "A: attempts per message, at most" }
      -- Retry extension: beta in us, then beta*2, beta*4, ...; contributes to Summary.timeUs.
      , backoff_us = { default = 10000, min = 0, max = 1000000, doc = "beta: wait beta 2^(j-1) before attempt j, us" }
      -- Brief: "every N seconds" -> T here, stored in ms; Python monitor_monotone checks snapshot gaps.
      , interval_ms = { default = 1000, min = 100, max = 60000, doc = "T: monitor period, ms" }
      -- Open arrivals: Poisson at lambda msg/s, keyed by the fate; 0 is the closed batch. Tested by little_law.
      , rate = { default = 0, min = 0, max = 1000000, doc = "lambda: arrivals per second, 0 = all at once" }
      -- Levels: P processes (or pods), each running M senders on the one queue.
      , workers = { default = 1, min = 1, max = 64, doc = "P: processes or pods, each with M senders" }
      }

let Property = < message_valid | fold_equivalence | bounded_memory | calibrated_fate | throughput_law | monitor_monotone | little_law >

let properties =
      -- Producer validity -> Model.hs prop_message_valid; Python test_message_valid.
      [ { id = Property.message_valid, claim = "Every message has 0 <= length <= 100 symbols from the alphabet and is a pure function of (seed, i)." }
      -- Exactly-once accounting -> Model.hs prop_fold_equivalence and the pipeline search; Python
      -- test_fold_equivalence (threads with a controller, processes, and pods on a real Valkey).
      , { id = Property.fold_equivalence, claim = "For any queue (threads, processes or a stream), any senders started and stopped during the run, a sender killed holding a message, and any interleaving, the committed total equals the pure fold run_range(0, N); a redelivered message counts once." }
      -- Queue/sender bound -> Model.hs pipeline inv and Python test_bounded_memory.
      , { id = Property.bounded_memory, claim = "At most C messages are queued and at most P M are in flight; the producer blocks at C, so the heap is O(C + M), independent of N." }
      -- Statistical calibration is checked in Python, not by the Haskell state search.
      , { id = Property.calibrated_fate, claim = "Over 2^16 messages, failure rates lie within Bernstein bounds of p and p^A and the mean latency within a Chernoff bound of mu, at delta = 1e-9; p = 0 and p = 1 are exact." }
      -- Two checks: Python measures virtual time; Haskell searches discrete transitions without a clock.
      , { id = Property.throughput_law, claim = "For the closed batch (lambda = 0), in virtual time the makespan T obeys max(S/M, max tau) <= T <= S/M + (1 - 1/M) max tau (Graham); in the model every reachable state drains." }
      -- Monitor behavior is tested in Python; the Haskell pipeline model has no monitor or clock.
      , { id = Property.monitor_monotone, claim = "The monitor sums each sender's latest report. Its snapshots never decrease, in virtual time each follows the previous one by T (the last by at most T), and the last equals the committed total." }
      -- Open arrivals and backpressure -> Python test_little_law, in virtual time.
      , { id = Property.little_law, claim = "With Poisson arrivals at lambda below the capacity M / E[tau], the rates in and out are within 15% of lambda; at 4x the capacity the queue fills to C and the rate in falls to the rate out, within 15% of the capacity." }
      ]

-- Mutation testing: intentionally break a design choice and require a check to detect the break.
let mutants =
      -- Model.hs lets Produce proceed past capacity C; bounded_memory must fail.
      [ { id = "UnboundedQueue", kills = Property.bounded_memory, change = "the producer ignores the queue bound C" }
      -- Model.hs requires queue room to requeue a retry; a full queue can leave every sender stuck.
      , { id = "RequeueRetry", kills = Property.throughput_law, change = "a failed attempt goes back on the bounded queue instead of staying with its sender" }
      -- Model.hs counts a message before its ack; a sender killed in between leaves it pending, and the redelivery counts again.
      , { id = "DoubleCommit", kills = Property.fold_equivalence, change = "the count and the ack are two steps, so a redelivered message counts twice" }
      -- Model.hs lets a sender read the queue head before another pops it; two senders then send one message.
      , { id = "DoubleTake", kills = Property.fold_equivalence, change = "the take is a read and then a pop, so two senders can take one message" }
      ]

-- These asserts are executable Dhall checks, unlike the claim strings above.
-- Dhall === asserts equivalence of normalized expressions; Haskell === builds a QuickCheck equality test.
let defaults_in_bounds =
      -- Fold all parameter entries with inBounds and Boolean AND, starting at True.
      assert : List/fold Entry (toMap params) Bool (\(e : Entry) -> \(ok : Bool) -> inBounds e.mapValue && ok) True === True

-- The bound check rejects an out-of-bounds default, so defaults_in_bounds can fail.
-- Record update // substitutes an invalid default only for this check; params itself is unchanged.
let the_bound_check_has_teeth = assert : inBounds (params.attempts // { default = 11 }) === False

-- One run's worst-case time total (something around ~4.2e18 us here at home) fits a signed 64-bit integer.
-- Natural/fold repeats multiplication by two k times starting at one: pow2 k = 2**k.
let pow2 = \(k : Natural) -> Natural/fold k Natural (\(x : Natural) -> x * 2) 1

-- Bound total service us across N messages, including worst-case retries and bounded sampled latency.
let worstTimeUs =
      -- Backoffs sum to beta*(2**(A-1)-1); each of A latencies is below 37*mu in this sampling scheme.
      params.n.max * (params.backoff_us.max * Natural/subtract 1 (pow2 (Natural/subtract 1 params.attempts.max)) + params.attempts.max * 37 * params.mean_us.max)

-- Require the computed upper bound to fit a signed 64-bit counter, useful for port to Rust
let totals_fit_in_i64 = assert : le worstTimeUs 9223372036854775807 === True
in  { max_len = 100, alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 .", params, properties, mutants }
