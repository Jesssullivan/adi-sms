# This file is the entrypoint for this repo.  Welcome!
# Please enter the toolchain with `nix develop`.
# You can invoke recipes such as `just test`; `just check` is the gate.
# 
# as restated throughout this project, I am using justfile as entrypoint 
# not because I think its needed here but as a demostration of stuff I do when left to my own habitat

set shell := ["bash", "-euo", "pipefail", "-c"]
export UV_LOCKED := "1" # every uv command fails if uv.lock is stale
job := env("JOB", "sms") # the run name

# Run pytest (the stream tests start their own valkey-server); HYPOTHESIS_PROFILE=deep runs 1000 examples, not 60
test *args:
    uv run pytest {{ args }}

# Compile the hs model then replay the vectors, then check its properties
model:
    mkdir -p .ghc && ghc -O1 -Wall -Werror -ispec/model -outputdir .ghc -o .ghc/model spec/model/Model.hs >/dev/null
    .ghc/model src/adi_sms/spec.json spec/generated/vectors.json

# Render spec/Spec.dhall (its asserts are re-checked) and diff it with spec.json; `just spec write` updates it
spec mode="check":
    if [[ {{ mode }} == write ]]; then dhall-to-json --file spec/Spec.dhall --output src/adi_sms/spec.json; fi
    dhall-to-json --file spec/Spec.dhall | diff -u src/adi_sms/spec.json -
    @echo "spec: rendering current"

# The thread and the process levels against the pure fold (`sms run` exits 1 when they differ)
levels *args="--n 2000 --mean-us 1000 --senders 20":
    uv run sms run --level thread {{ args }} 2>/dev/null
    uv run sms run --level process --workers 3 {{ args }} 2>/dev/null

# gates that are just always good 
check: lint test model spec levels
    gitleaks git --no-banner --redact --log-level warn .
    for part in queue producer senders; do just render $part example.org/sms:1 --n=9; echo ---; \
        PULL_SECRET=pull just render $part example.org/sms:1; echo ---; done | kubeconform -strict -summary -kubernetes-version 1.33.0
    @echo "check: ok"

# Ruff's lint and format check on FILES (the pre-commit hook passes the staged ones); `just fmt` fixes both
lint *files="src tests":
    ruff check -q {{ files }}
    ruff format --check -q {{ files }}

# Apply ruff's safe fixes, then its format
fmt *files="src tests":
    ruff check -q --fix {{ files }}
    ruff format -q {{ files }}

# Run `just lint` on the staged Python before every commit (.githooks/pre-commit)
hooks:
    git config core.hooksPath .githooks

# Render deploy/PART.yaml (queue, producer or senders) for IMAGE and FLAGS; P, QUEUE, PULL_SECRET from the env
render part image *flags:
    @grep -v '^ *#' deploy/{{ part }}.yaml | JOB={{ quote(job) }} P={{ quote(env("P", "3")) }} IMAGE={{ quote(image) }} \
        QUEUE={{ quote(env("QUEUE", "192.0.2.1:6379")) }} PULL_SECRET={{ quote(env("PULL_SECRET", "")) }} \
        FLAGS={{ quote(replace(prepend(', "', append('"', flags)), '" ,', '",')) }} \
        envsubst '$JOB $P $IMAGE $QUEUE $PULL_SECRET $FLAGS' | grep -v 'name: ""'
