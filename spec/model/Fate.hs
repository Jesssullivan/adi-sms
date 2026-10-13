-- | The fate, restated bit for bit from src/adi_sms/fate.py (the reference).
-- spec/generated/vectors.json judges this file. Every random choice is a 
-- pure function of (seed, message i, attempt j, purpose): SplitMix64's
-- finalizer as a keyed hash, with no generator state. Word64 wraps mod 2^64 as Python's `& MASK` does.
-- Reading: Spec.dhall -> spec.json -> Model.hs Params generators -> this pure fate -> Summary.
-- Python equivalents: src/adi_sms/fate.py key/body/deliver/run_range.
module Fate where

import Data.Bits (shiftL, shiftR, xor, (.&.), (.|.))
import Data.List (genericLength, genericTake)
import Data.Word (Word64)
import Numeric (log1p)

-- | A Config's fate fields. `maxAttempts`, not `attempts`: Summary owns that name.
-- Params contains only values that affect fate, eg. seed, latency mean, failure rate, attempts, backoff.
-- M/C/T affect scheduling/reporting, so they are absent. Model.hs paramsG reads bounds from the spec.
-- ! marks fields strict; deriving Show makes generated parameters printable in a failed test.
data Params = Params {seed :: !Word64, meanUs, failPpm, maxAttempts, backoffUs :: !Integer} deriving (Eq, Show)

-- | The commutative monoid every count lives in, in JSON order (sent, failed, attempts, time_us).
-- Brief: sent/failed totals and average time. Store additive time and counts; derive averages later.
-- Integer counters are arbitrary precision. deriving Eq/Ord supports comparisons and model-state map keys.
data Summary = Summary {sent, failed, attempts, timeUs :: !Integer} deriving (Eq, Ord, Show)

-- MONOID OPERATION: <> adds the four matching fields, as fate.py Summary.__add__ does.
-- Example: Summary 1 0 1 10 <> Summary 0 1 2 30 = Summary 1 1 3 40.
instance Semigroup Summary where Summary a b c d <> Summary a' b' c' d' = Summary (a + a') (b + b') (c + c') (d + d')

-- MONOID IDENTITY: mempty is zero; x <> mempty = x and mempty <> x = x.
-- Integer addition also commutes, so arrival order does not change totals.
-- Model.hs prop_fold_equivalence checks partitions, shuffled parts and both identity directions.
instance Monoid Summary where mempty = Summary 0 0 0 0

-- Type signature shared by six constants: all are Word64.
maxLen, pLen, pBody, pLat, pFail, pArr :: Word64
-- Brief: up to 100 characters; purpose tags separate length/body/latency/failure/arrival draws.
(maxLen, pLen, pBody, pLat, pFail, pArr) = (100, 1, 2, 3, 4, 5) -- spec.json's max_len, then the purpose tags

-- | SplitMix64's finalizer: a bijection on 64-bit words that spreads every input bit.  This is really cool.
-- One Word64 in --> one Word64 out- Model.hs vectorsP compares fixed known input/output pairs.
mix :: Word64 -> Word64
mix z = y `xor` (y `shiftR` 31)
  where x = (z `xor` (z `shiftR` 30)) * 0xBF58476D1CE4E5B9
        y = (x `xor` (x `shiftR` 27)) * 0x94D049BB133111EB

-- Both absorb and key take two Word64 inputs and return one Word64.
absorb, key :: Word64 -> Word64 -> Word64
-- Combine prior hash and input word, add the golden increment, then mix.
absorb h w = mix ((h `xor` w) + 0x9E3779B97F4A7C15)
-- Partial application leaves the ID argument for later: key s i = absorb (absorb 0 s) i.
key s = absorb (absorb 0 s) -- message i's key: the seed and the id are absorbed in separate rounds

-- draw takes message key, purpose tag and counter; returns a deterministic 64-bit word.
draw :: Word64 -> Word64 -> Word64 -> Word64
-- BIT LAYOUT: [ purpose: bits 63..56 | counter: bits 55..0 ].
-- shiftL 56 places the tag; .|. inserts the small counter. No shared RNG is advanced.
draw k p j = absorb k (p `shiftL` 56 .|. j)

-- Length returns Int so take can use it; its input is the already-derived message key.
len :: Word64 -> Int
-- mod 101 yields lengths 0..100 (with modulo bias below 2**-64); see prop_message_valid.
len k = fromIntegral (draw k pLen 0 `mod` (maxLen + 1)) -- U{0..100}

-- | Message i's text: the little-endian bytes of its BODY draws; each byte's low 6 bits pick a symbol.
-- Alphabet and (seed, ID) become a String; the alphabet comes from the rendered spec.
body :: String -> Word64 -> Word64 -> String
-- List comprehension: draw body words, then extract each byte via shiftR and .&. 63.
-- The low six bits select one of 64 symbols via !!; take keeps exactly the chosen message length.
-- Laziness means the infinite [0..] word stream is consumed only as far as take needs.
body abc s i = take (len k) [abc !! fromIntegral (w `shiftR` r .&. 63) | w <- map (draw k pBody) [0 ..], r <- bytes]
  -- Offsets 0,8,...,56 read low byte first, matching Python to_bytes(8, "little").
  where (k, bytes) = (key s i, [0, 8 .. 56]) -- bytes: the bit offsets of a word's 8 little-endian bytes

-- | Exp(mean) by inversion of an exact 53-bit uniform in [0, 1), floored to whole microseconds.
-- Input mean and output time are Integer microseconds; intermediate logarithm uses Double.
latencyUs :: Word64 -> Word64 -> Integer -> Integer
-- Inverse exponential CDF -mu*ln(1-u), then floor to integer us; Python calibrated_fate tests the mean.
latencyUs k j mu = floor (fromInteger (negate mu) * log1p (negate u))
  -- Discard 11 bits; encodeFloat x (-53) gives x*2**(-53), a 53-bit uniform in [0,1).
  where u = encodeFloat (toInteger (draw k pLat j `shiftR` 11)) (-53) :: Double

-- | An exact integer Bernoulli(ppm / 10^6).
-- Input ppm is an Integer; the Boolean tells whether this attempt fails.
fails :: Word64 -> Word64 -> Integer -> Bool
-- Compare the draw with floor(ppm*2**64/1000000); ppm=0 and ppm=1000000 are exact endpoints.
fails k j ppm = toInteger (draw k pFail j) < (ppm `shiftL` 64) `div` 1000000

-- | Message i's whole fate before it starts: each attempt's wait (us), and the outcome. At most A attempts;
-- attempt j >= 1 first backs off beta 2^(j-1) (2^j `div` 2 is 0 at j = 0). Retries stay with the sender.
-- deliver returns (attempt waits in us, one message Summary), like fate.py deliver.
deliver :: Params -> Word64 -> ([Integer], Summary)
-- ok is 0 or 1; total attempts count waits, total service time sums them, including backoffs.
deliver p i = (waits, Summary ok (1 - ok) (genericLength waits) (sum waits))
  -- Bind the stable key shared by this message's attempts.
  where k = key (seed p) i
        -- Take at most A attempt indices. span splits at the first success into failed prefix bad and rest good.
        -- This stops the failure scan at the first success; later attempts do not contribute.
        (bad, good) = span (\j -> fails k j (failPpm p)) (genericTake (maxAttempts p) [0 ..])
        -- No successful remainder means terminal failure; otherwise the message counts as sent.
        ok = if null good then 0 else 1
        -- Waits cover all failures before success plus at most one successful attempt; j=0 gives zero backoff.
        waits = [backoffUs p * (2 ^ j `div` 2) + latencyUs k j (meanUs p) | j <- bad ++ take 1 good]

-- | The pure fold over ids [a, b): runRange p a c == runRange p a b <> runRange p b c.
-- The ID interval is [a,b), with b excluded; result is a Summary rather than a list of messages.
runRange :: Params -> Word64 -> Word64 -> Summary
-- foldMap maps each ID to its Summary (snd selects the tuple's second element), then combines with <>.
-- An empty range yields mempty. This is the Haskell form of fate.py run_range's sum(..., Summary()).
runRange p a b = foldMap (snd . deliver p) (takeWhile (< b) [a ..])
