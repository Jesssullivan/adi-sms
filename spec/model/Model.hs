{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Data.Aeson (Value, eitherDecodeFileStrict, withObject, (.:))
import Data.Aeson.Types (Parser, parseEither)
import Data.List (delete, genericLength, insert, intercalate, nub, sort, transpose)
import qualified Data.Map.Strict as Map
import Data.Word (Word64)
import Fate
import System.Environment (getArgs)
import System.Info (os)
import Test.QuickCheck
import Test.QuickCheck.Random (mkQCGen)
import Text.Printf (printf)

-- | Every golden vector, as one conjunction. log1p is bit-identical on one glibc; elsewhere a latency may be 1 us off.
-- Read golden examples into one Property plus a case count; Parser can report malformed JSON.
vectorsP :: String -> Value -> Parser (Int, Property)
vectorsP abc = withObject "vectors" $ \o -> do
  o .: "schema" >>= \v -> if v == ("adi-sms-vectors-v2" :: String) then pure () else fail ("schema " <> v)
  (mu, ppm) <- (,) <$> o .: "mean_us" <*> o .: "fail_ppm"
  let tol = if os == "linux" then 0 else 1 -- us per attempt
      -- Local helper: read an array, require each member to be an object and parse/test each member.
      each k f = o .: k >>= mapM (withObject "vector" f)
  mixes <- each "mix" $ \m -> (\z out -> mix z === out) <$> m .: "in" <*> m .: "out"
  -- For each message vector, parse its seed, ID, latency list and expected structural values.
  cases <- each "cases" $ \c -> do
    (s, i, lat) <- (,,) <$> c .: "seed" <*> c .: "i" <*> c .: "lat"
    want <- (,,,) <$> c .: "key" <*> c .: "len" <*> c .: "body" <*> c .: "fail"
    -- pure lifts the test into Parser: key, length, text and failures match, and all 3 latencies within tol.
    pure $ let k = key s i in want === (k, len k, body abc s i, [fails k j ppm | j <- [0 .. 2]])
      .&&. length lat == 3 && and (zipWith (\x y -> abs (x - y) <= tol) lat [latencyUs k j mu | j <- [0 .. 2]])
  -- v2: each case's raw arrival draw, the 64-bit word gap_s turns into a wait, in case order.
  arrs <- o .: "arr" :: Parser [Word64]
  arr <- (arrs ===) <$> each "cases" (\c -> (\s i -> draw (key s i) pArr 0) <$> c .: "seed" <*> c .: "i")
  -- Whole-range vectors also compare the fold, so they exercise aggregation as well as individual draws.
  runs <- each "runs" $ \r -> do
    c <- r .: "cfg"
    -- Construct Params from a vector's settings, independent of randomly generated cases.
    p <- Params <$> c .: "seed" <*> c .: "mean_us" <*> c .: "fail_ppm" <*> c .: "attempts" <*> c .: "backoff_us"
    (a, b, want) <- (,,) <$> r .: "a" <*> r .: "b" <*> r .: "summary"
    -- Require 4 entries: exact sent/failed/attempt counts, with a per-attempt tolerance for accumulated time.
    pure $ let Summary s f n t = runRange p a b in length want == 4 .&&. take 3 want === [s, f, n]
      .&&. abs (t - want !! 3) <= tol * n
  -- conjoin requires every vector property to pass; main wraps this fixed suite in once.
  pure (length mixes + length cases + length runs, conjoin (mixes <> cases <> runs <> [arr]))

-- | What the model reads from spec.json: the alphabet, max_len, every parameter's [min, max] and the mutants.
-- Spec retains alphabet, max length, parameter bounds and mutant names/property IDs from JSON.
data Spec = Spec {alphabet :: String, specMaxLen :: Word64, bounds :: Map.Map String (Integer, Integer),
                  mutants :: [(String, String)]}

-- This parser links the Dhall-generated data to the executable Haskell tests.
specP :: Value -> Parser Spec
specP = withObject "spec" $ \o -> Spec <$> o .: "alphabet" <*> o .: "max_len"
  -- Read min/max into the map used by paramsG; read mutant id/kills pairs used by main.
  <*> (pairs "min" "max" =<< o .: "params") <*> (pairs "id" "kills" =<< o .: "mutants")
  -- traverse parses every entry while preserving the surrounding map/list structure.
  where pairs a b = traverse (withObject "entry" $ \q -> (,) <$> q .: a <*> q .: b)

-- | A fate anywhere in the spec's bounds, and often exactly on one (p = 0, p = 1, A = 1, A = 10, mu = 0).
-- Gen Params is a generator, not a concrete Params value; forAll runs it to obtain cases.
paramsG :: Spec -> Gen Params
-- Compose generators for the five fate settings; seed is converted to Word64.
paramsG sp = Params . fromInteger <$> f "seed" <*> f "mean_us" <*> f "fail_ppm" <*> f "attempts" <*> f "backoff_us"
  -- Look up a field's bounds; choose samples within them, elements chooses either endpoint.
  -- oneof chooses between these recipes, deliberately giving boundary cases frequent attention.
  where f k = let r = bounds sp Map.! k in oneof [choose r, elements [fst r, snd r]]

-- | 0 <= length <= max_len and alphabet symbols only, for any in-bounds seed and any 64-bit id.
-- Brief: random messages up to 100 characters; this law checks alphabet/length consistency.
prop_message_valid :: Spec -> Property
-- forAll supplies a generated seed, then a generated 64-bit ID, to the property function.
prop_message_valid sp = forAll (seed <$> paramsG sp) $ \s -> forAll (choose (minBound, maxBound)) $ \i ->
  -- counterexample adds the generated body to diagnostics; compare the model limit with the spec.
  let (abc, m) = (alphabet sp, body abc s i) in counterexample m $ maxLen == specMaxLen sp && length abc == 64
    -- Check alphabet uniqueness, derived length, maximum length and membership of every character.
    && nub abc == abc && length m == len (key s i) && length m <= fromIntegral maxLen && all (`elem` abc) m

-- | The monoid law: the parts of any partition of [a, c) into id ranges, summed in any order, give runRange p a c,
-- which counts each id once with 1..A attempts; and mempty is the identity.
prop_fold_equivalence :: Spec -> Property
-- Generate Params, a safe starting ID and a list of cut offsets while leaving room to avoid Word64 overflow
prop_fold_equivalence sp = forAll ((,,) <$> paramsG sp <*> choose (0, maxBound - 256) <*> listOf (choose (0, 128))) $
  -- Sort cuts, pair neighbors into [lo,hi) ranges, and fold each part. Duplicate cuts make empty ranges.
  -- Haskell let bindings are recursive: c can use cuts even though both are declared in this let.
  \(p, a, ds) -> let (cuts, c) = (a : sort (map (a +) ds), last cuts); parts = zipWith (runRange p) cuts (drop 1 cuts)
                     -- Keep the whole Summary and destructure sent/failed/attempts for count constraints.
                     whole@(Summary s f n _) = runRange p a c
   -- Shuffle part summaries; mconcat folds <> from mempty. Require both orders to equal whole.
   in forAll (shuffle parts) $ \mixed -> mconcat parts === whole .&&. mconcat mixed === whole <> mempty
        -- Also check left identity, one terminal outcome per ID and between one and A attempts per message.
        .&&. mempty <> whole === whole .&&. s + f == toInteger (c - a) && s + f <= n && n <= maxAttempts p * (s + f)

-- | Breadth-first search by level: level k holds every state first reached in k moves, in state order, each with a
-- shortest trace to it (newest move first); level 0 is s0. 
levels :: Ord s => s -> (s -> [(a, s)]) -> [[(s, [a])]]
levels s0 next = go (Map.singleton s0 []) [(s0, [])]
  -- Expand one level: prefix each move to its trace, drop seen states; a state reached twice keeps one shortest trace.
  where go seen level = level : if null new then [] else go (seen <> new) (Map.toList new) -- ends when none is new
          where new = Map.fromList [(t, a : p) | (s, p) <- level, (a, t) <- next s] `Map.difference` seen

-- | Each mutant reverses one design decision and must break the property that pins it.
data Mutant = Correct | UnboundedQueue | RequeueRetry | DoubleCommit | DoubleTake deriving (Eq, Read)

-- The moves: the producer adds the next id; a sender takes the head (the read and the pop are one move unless
-- DoubleTake); an attempt fails and is retried; the last attempt commits (the ack and the count, one script); a
-- count is acked (DoubleCommit only); a sender dies holding id i; a sender starts; a live sender reclaims an
-- orphaned id (XAUTOCLAIM after the lease).
data Step = Arrive | Take | Pop | Retry Word64 | Commit Word64 | Ack Word64 | Kill Word64 | Spawn | Claim Word64
  deriving (Show)

-- | Producer -> queue of C -> senders (1..M live) -> booked total. Queue entries are (id, attempt); held is the
-- multiset of messages in senders' hands as (id, attempt, counted); orphans are pending ids whose sender died.
-- peeks counts senders that copied the queue head without popping it (only DoubleTake reads without popping).
-- One kill and one spawn per run keep the search finite. The commit is atomic: the count and the ack are one move.
data Pipe = Pipe {made :: Word64, queued :: [(Word64, Int)], peeks :: Int, held :: [(Word64, Int, Bool)],
                  orphans :: [Word64], live, kills, spawns :: Int, booked :: Summary} deriving (Eq, Ord)

pipeline :: Mutant -> (Word64, Int, Int, Params) -> [[([String], [Step])]]
pipeline mu (n, c, m, p) = [[(inv s (genericLength t), t) | (s, t) <- l] | l <- levels (Pipe 0 [] 0 [] [] m 0 0 mempty) next]
  where final i j = j + 1 == length (fst (deliver p i)) -- its last attempt: it was sent, or it was the A-th
        room s = length (queued s) < c
        free s = length (held s) < live s -- a live sender has empty hands
        next s = [(a, step s a) | a <- [Arrive | made s < n, mu == UnboundedQueue || room s]
          ++ [Take | not (null (queued s)), free s, peeks s < 2]
          ++ [Pop | peeks s > 0]
          ++ [if final i j then Commit i else Retry i | (i, j, False) <- nub (held s),
              mu /= RequeueRetry || final i j || room s]
          ++ [Ack i | (i, _, True) <- nub (held s)]
          ++ [Kill i | kills s < 1, live s > 1, (i, _, _) <- nub (held s)]
          ++ [Spawn | spawns s < 1, live s < m]
          ++ [Claim i | i <- nub (orphans s), free s]]
        step s Take | mu == DoubleTake = s {held = hold, peeks = peeks s + 1} -- the read; the pop is a later move
                    | otherwise = s {held = hold, queued = drop 1 (queued s)} -- read and pop in one atomic move
          where hold = foldr (\(i, j) -> insert (i, j, False)) (held s) (take 1 (queued s))
        step s Pop = s {queued = drop 1 (queued s), peeks = 0}
        step s (Retry i) = step s (Commit i) -- not its last attempt: the branches below tell them apart
        step s (Commit i) | final i j && mu == DoubleCommit = s {held = insert (i, j, True) rest, booked = add}
                          | final i j = s {held = rest, booked = add} -- the ack and the count, one transaction
                          | mu == RequeueRetry = s {held = rest, queued = queued s ++ [(i, j + 1)]}
                          | otherwise = s {held = insert (i, j + 1, False) rest}
          where (j, rest) = pick i s
                add = booked s <> snd (deliver p i)
        step s (Ack i) = s {held = snd (pick i s)}
        step s (Kill i) = s {held = snd (pick i s), orphans = insert i (orphans s), live = live s - 1, kills = 1}
        step s Spawn = s {live = live s + 1, spawns = 1}
        step s (Claim i) = s {orphans = delete i (orphans s), held = insert (i, 0, False) (held s)} -- from attempt 0
        step s _ = s {made = made s + 1, queued = queued s ++ [(made s, 0)]} -- Arrive
        pick i s = case [h | h@(k, _, _) <- held s, k == i] of -- one hand holding i: its attempt, and the rest
          e@(_, j, _) : _ -> (j, delete e (held s))
          [] -> (0, held s)
        done s = made s == n && null (queued s) && null (held s) && null (orphans s)
        inv s k = ["bounded_memory" | length (queued s) > c || length (held s) > live s || live s > m]
          ++ ["fold_equivalence" | done s, booked s /= runRange p 0 n]
          ++ ["throughput_law" | not (done s) && null (next s) || k > toInteger n * (2 * maxAttempts p + 2) + maxAttempts p + 3]

-- | The search levels across the shapes: level k holds, in grid order, each shape's states first reached in k moves
-- (transpose skips a shape whose search has ended). The shapes: pipelines with N <= 6, C <= 3, M <= 3, A <= 2 and p in
-- {0, 1/2, 1}, so never and always fail (at p = 1/2, seed 1 retries ids 1, 2 and 5).
grid :: Mutant -> [[(String, [([String], [Step])])]]
grid mu = transpose [(,) (printf "N=%d C=%d M=%d A=%d p=%d" n c w a q) <$> pipeline mu (n, c, w, Params 1 1000 q a 0)
  | n <- [0 .. 6], c <- [1 .. 3], w <- [1 .. 3], a <- [1, 2], q <- [0, 500000, 1000000]]

-- | A QuickCheck Property: no state of the grid breaks `name`, or a shortest trace to one. The list is lazy and read
-- level by level, so a mutant's search stops at the first level that breaks `name` and never builds the deeper ones.
searched :: [[(String, [([String], [Step])])]] -> String -> Property
searched g name = case [(sh, reverse t) | k <- g, (sh, l) <- k, (b, t) <- l, name `elem` b] of
  -- No violations: every level ran to the end; label the passing result with the number of states checked.
  [] -> label (printf "%d states" (sum [length l | k <- g, (_, l) <- k])) True
  -- A violation: the first broken state of the first broken level, in grid order, then in state order.
  (sh, t) : _ -> counterexample (printf "%s in %d moves: %s" sh (length t) (intercalate ", " (map show t))) False

-- EXECUTION: command arguments -> JSON parsers -> test suite -> printed verdicts -> exit success/failure.
main :: IO ()
main = do
  -- The model executable expects two paths: rendered spec and golden vectors.
  [spec, vectors] <- getArgs
  -- Load spec data used for generators and mutant expectations.
  sp <- load spec specP
  -- Load the fixed golden-vector Property using the spec alphabet.
  (nv, vs) <- load vectors (vectorsP (alphabet sp))
  -- Run each named test via verdict; once runs the fixed vectors as one test.
  ok <- mapM verdict $ [(printf "vectors (%d)" nv, once vs), ("message_valid", prop_message_valid sp),
      -- Combine sampled fold laws with the finite correct-model fold search.
      ("fold_equivalence", prop_fold_equivalence sp .&&. searched correct "fold_equivalence")]
    -- Run bounded_memory and throughput_law searches once: repeating deterministic enumeration adds no cases.
    <> [(v, once (searched correct v)) | v <- ["bounded_memory", "throughput_law"]]
    -- Read each mutant named by Dhall, require its designated property to fail using expectFailure.
    -- A detected defect is the intended success here; if the mutant survives, this expectation fails.
    <> [(m <> " kills " <> v, once (expectFailure (searched (grid (read m)) v))) | (m, v) <- mutants sp]
  -- Every verdict must succeed; otherwise fail the executable so the check command reports failure.
  if and ok then pure () else fail "a property or a mutant check failed"
  where
    correct = grid Correct -- one search serves the three properties of the correct pipeline
    -- Decode a file then parse its structure; turn either decoding or parsing errors into IO failure.
    load path p = eitherDecodeFileStrict path >>= either fail pure . (>>= parseEither p)
    -- Each property gets a fresh runner with the same replay seed; this is separate from the SMS seed.
    verdict (name, prop) = do -- a fixed seed: the same 1000 tests every run
      -- maxSuccess requests 1000 successful cases for sampled properties; once overrides fixed checks.
      -- chatty=False captures output instead of printing it immediately; replay fixes the generator seed.
      r <- quickCheckWithResult stdArgs {replay = Just (mkQCGen 20261009, 0), maxSuccess = 1000, chatty = False} prop
      -- Print one named result line; return its success flag for the final and check.
      putStrLn (name <> ": " <> unwords (lines (output r))) >> pure (isSuccess r)
