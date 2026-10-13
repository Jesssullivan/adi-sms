Hello friends!

This repository contains my solution to the SMS simulation challenge, received Oct. 9th, written with Python 3.15 (which happens to have also been released Oct 9th; find my use of the new and wonderful builtin immutable type  `frozendict` from [PEP 814](https://peps.python.org/pep-0814/)). :-)


If you'd like to dive right into my interpreted solution code, you may want to beeline into the `./src/adi_sms/` python package, which is effectively two files, `fate.py` and `pipeline.py`.  However, I think it'll be more illuminating to start in my [design document](docs/02-design.svx) or [better yet read the docs](https://adi.clients.xoxd.ai/docs/overview), as I've done my best to walk through (and justify) my approach and decision making.  Or, jump to the [quickstart below](#quickstart)

Find my the svx source for the docs and disclosures in `./docs/`. The benchmarks live in [adi-sms-ports/bench](https://github.com/Jesssullivan/adi-sms-ports/tree/main/bench).  These are pinned and deployed automatically to the live rendered site.

If you'd like to explore my solution's functional specification, along with my QuickCheck backed PBT model implemented in Haskell, please explore `./spec/`.  You can learn more about [Dhall here](https://github.com/dhall-lang/dhall-lang) and [QuickCheck](https://www.cse.chalmers.se/~rjmh/QuickCheck/manual.html) here.  Indeed, the Python package for PBT testing I've used in this solution [Hypothesis](https://hypothesis.readthedocs.io/) is modeled after QuickCheck, and is how I tend to [approach problem solving](docs/03-properties.svx) if at all possible. 

## Scope

This solution has been implemented to scale from a single multithreaded execution running locally (where there can be no fewer than three concurrent threads: the Producer, at least one Sender, and the Monitor) all the way out to M sender threads in each of P Kubernetes pods, all on one shared [Valkey stream](https://github.com/valkey-io), which is the Linux Foundation's open source implementation of Redis. On the live demo, the demo server [actaully scales the pods](https://adi.clients.xoxd.ai/docs/scaling).

I should note I used the Redis branded `redis-py` (and the Rust port uses the redis crate), which are functionally equivalent but on hindsite should have used LF's valkey-py to keep the branding for readability, as I make no assumption everyone everywhere regularly stays up late fretting about FOSS K/V stores 👀

I want to note up front, despite all the fun of scale, throughput is fundamentally limited by message time in flight, which is a configurable axiom of the exercise brief (the mean send time).  

You are invited to play with the [live demo](https://adi.clients.xoxd.ai/demo) running live on [hardware in my basement](docs/07-machines.svx), and walk through [my slides](https://adi.clients.xoxd.ai).  You must sign in using the cloudflare OTC, and can only do so if your email domain is on my whitelist (or are actually me, in which case you'd use the tsidp login).

I've created a supplementary whiteboard video for this project, running through the initial thought processes and design of this system for a literal fly-on-the-wall view into me literally doing what I do when approaching a novel problem.  This was cooler in my head; the whitebaord session in which I painfully worked through some monads, QuickMix64 and Bernoulli distribution ended up failing ~10 minutes in due to the video-taking laptop hitting ENOSPEC, which itself is quite embarrassing. 


## quickstart

Clone the repository (or open it in GitHub Codespaces if you're in a rush)

With Nix flakes, run every check:

```sh
nix develop -c just check
nix develop -c just hooks # ruff checks the staged Python before every commit
```

Without Nix you should install uv which'll fetch CPython 3.15 and the various frozen deps:

```sh
## canonical uv curl to bash warning :-\
curl -LsSf https://astral.sh/uv/install.sh | sh && . "$HOME/.local/bin/env"

alias sms='uv run --locked --managed-python sms'

sms fold          # the Summary every run must reach
sms run --help    # the flags
```


The monitor lines are logged to stderr; then each run prints its Summary, and `"check": true` when the Summary equals the fold.

```sh
# threads; then one rate in msg/s per third of the ids
sms run --level thread --senders 10
sms run --level thread --rate 50,400,50
sms fold
```

```sh
# procs: P processes of M sender threads
sms run --level process --workers 3 --senders 4
sms run --level process --workers 3 --senders 4 --rate 100
sms fold
```

Pods need an empty Valkey per run. Without Nix, use `docker run --rm -p 6379:6379 valkey/valkey:9`.

```sh
# pods, locally; Ctrl-C on the monitor halts the run
valkey-server --save "" --port 6379 &
sms consume --queue 127.0.0.1:6379 --name a & sms consume --queue 127.0.0.1:6379 --name b &
sms produce --queue 127.0.0.1:6379 --rate 200 & sms monitor --queue 127.0.0.1:6379
# from another shell, stop consumer a or start a third; "check" stays true
pkill -TERM -f 'consume.*--name a$'
```

On Kubernetes the demo server creates the three Jobs in `deploy/` (see `just render`) and scales the senders Job.


--- 

Upon completing the functional specification, I thought it'd be fun to point claude at this to reimplement in Rust and Haskell and expand + create multithread executables, from which I have established and published fold operation benchmarks against my python implementation; find them [here](https://github.com/Jesssullivan/adi-sms-ports/tree/main/bench), checked byte for byte against `sms fold`. In defense of my humanity, I (like everyone else) *am* gaining comfort writing, reading and reviewing Rust, but I felt it would be best to stick with Python for my own confidence, ability to communicate and the establishment of the functional model with familiar PBT packages. 

> Regarding the above, Gabriella Gonzalez (author of the Dhall language, among many other cool things) wrote a brief blog post relating to this stance on use of code generation with LLMs https://haskellforall.com/2026/03/a-sufficiently-detailed-spec-is-code

#### More resources 
Similarly, if all this folding and this monoid nonsense feels sketchy and scary, I suggest watching [Antoine Leblanc’s video essay on Monads](https://www.youtube.com/watch?v=LekhueQ4zVU) as a good place to begin.  Similarly, I suggest reading through their blog https://nauths.fr/en/ as well as Gabby’s blog https://haskellforall.com/ for general discussions involving Haskell and functional programming paradigms.

### Repo Layout

| path | contents |
|---|---|
| `src/adi_sms/` | `fate.py`, `pipeline.py`, `commit.lua`, `__main__.py` (the `sms` command) and the rendered `spec.json` |
| `tests/test_properties.py` | the seven properties as Hypothesis tests |
| `spec/Spec.dhall` | parameters, bounds, property names and mutants |
| `spec/model/` | the Haskell model and fate |
| `spec/generated/vectors.json` | golden vectors the tests and the model replay |
| `deploy/` | the queue, producer and senders Job templates |
| `Justfile` | the recipes; `just check` is the gate |
| `flake.nix`, `flake.lock`, `pyproject.toml`, `uv.lock` | the pinned toolchain and checks; the package and its locked deps |
| `.github/workflows/ci.yml` | CI: `nix flake check`, then `just check` |
| `docs/` | the docs the slide site renders and the data-flow diagram |
