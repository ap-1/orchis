{ pkgs, config, ... }:

{
  languages.rust = {
    enable = true;
    channel = "nightly";
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
      "rust-src"
    ];
  };

  services.redis = {
    enable = true;
    port = 6399;
  };

  env.ORCHIS_TEST_REDIS = "1";
  env.ORCHIS_TEST_REDIS_URL = "redis://127.0.0.1:6399";

  enterTest = ''
    cargo test --workspace
  '';

  treefmt = {
    enable = true;
    config.programs = {
      nixpkgs-fmt.enable = true;
      rustfmt.enable = true;
    };
  };

  git-hooks.hooks = {
    treefmt.enable = true;
    clippy = {
      enable = true;
      packageOverrides.cargo = config.languages.rust.toolchainPackage;
      packageOverrides.clippy = config.languages.rust.toolchainPackage;
    };
  };
}
