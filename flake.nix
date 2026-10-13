{
  description = "adi-sms is an SMS simulation built around some PBTs and a commutative monoid ^w^";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/e7439b6b14ad3cc35d05608ebca9bce01a25f5f8";
    pyproject-nix = {
      url = "github:pyproject-nix/pyproject.nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    uv2nix = {
      url = "github:pyproject-nix/uv2nix";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    pyproject-build-systems = {
      url = "github:pyproject-nix/build-system-pkgs";
      inputs.pyproject-nix.follows = "pyproject-nix";
      inputs.uv2nix.follows = "uv2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    inputs@{ nixpkgs, ... }:
    let
      inherit (nixpkgs) lib;
      # CPython 3.15.0 for the pinned set, whose python315 is 3.15.0rc2. As an overlay (not a bare override) the
      # build-time interpreters follow it too, and the derivation is the one binary caches already hold.
      python315 = _final: prev: {
        python315 = prev.python315.override {
          sourceVersion = {
            major = "3";
            minor = "15";
            patch = "0";
            suffix = "";
          };
          hash = "sha256-ukvtG6NGuRaJC3bZ4yBFFCCqafZAiZfTPGZILurj1XU=";
        };
      };
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      perSystem = f: lib.genAttrs systems (system: f (nixpkgs.legacyPackages.${system}.extend python315));
      ghc = pkgs: pkgs.haskell.packages.ghc9103.ghcWithPackages (p: [ p.QuickCheck p.aeson ]);
      env =
        pkgs: python: groups:
        let
          workspace = inputs.uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };
          base = pkgs.callPackage inputs.pyproject-nix.build.packages { inherit python; };
          pythonSet = base.overrideScope (
            lib.composeManyExtensions [
              inputs.pyproject-build-systems.overlays.wheel
              (workspace.mkPyprojectOverlay { sourcePreference = "wheel"; })
              # pyyaml has no 3.15 wheel: its sdist builds with setuptools (the tests read deploy/ with it)
              (final: prev: {
                pyyaml = prev.pyyaml.overrideAttrs (o: {
                  nativeBuildInputs = o.nativeBuildInputs ++ final.resolveBuildSystem { setuptools = [ ]; };
                });
              })
            ]
          );
        in
        pythonSet.mkVirtualEnv "adi-sms-env" { adi-sms = groups; };
      # A check passes when the script exits 0
      check =
        pkgs: name: deps: script:
        pkgs.runCommand "adi-sms-${name}" { nativeBuildInputs = deps; } ''
          cp -r ${./.} repo && chmod -R u+w repo && cd repo && export HOME=$TMPDIR XDG_CACHE_HOME=$TMPDIR
          ${script}
          touch $out
        '';
    in
    {
      checks = perSystem (pkgs: {
        # the committed rendering equals a fresh render of the spec, whose asserts are re-checked
        spec = check pkgs "spec" [ pkgs.dhall-json ] "dhall-to-json --file spec/Spec.dhall | diff -u src/adi_sms/spec.json -";
        model = check pkgs "model" [ (ghc pkgs) ] ''
          ghc -O1 -Wall -Werror -ispec/model -outputdir $TMPDIR/ghc -o $TMPDIR/model spec/model/Model.hs
          $TMPDIR/model src/adi_sms/spec.json spec/generated/vectors.json
        '';
        tests = check pkgs "tests" [ (env pkgs pkgs.python315 [ "dev" ]) pkgs.valkey ] "python -m pytest -p no:cacheprovider";
      });

      # the runtime venv (bin/python, bin/sms) for the image
      packages = perSystem (pkgs: { default = env pkgs pkgs.python315 [ ]; });

      devShells = perSystem (pkgs: {
        default = pkgs.mkShell {
          packages = [
            pkgs.python315
            pkgs.uv
            pkgs.just
            pkgs.dhall-json # `just spec`
            (ghc pkgs) # `just model`
            pkgs.gettext # `just render` fills the templates with envsubst
            pkgs.kubeconform # `just check` validates the rendered Job
            pkgs.gitleaks # `just check` scans the history for secrets
            pkgs.ruff # `just lint` and `just fmt`
            pkgs.valkey # the stream tests start a local queue
          ];
          env = {
            UV_PYTHON = "${pkgs.python315}/bin/python3.15"; # one interpreter (and one libm) everywhere
            UV_PYTHON_DOWNLOADS = "never";
            UV_PYTHON_PREFERENCE = "only-system";
          };
        };
      });
    };
}
