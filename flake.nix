{
  description =
    "event-sorcery: a Rust event-sourcing library on top of cqrs-es.";

  inputs = {
    rainix.url =
      "github:rainprotocol/rainix?rev=560ee6ec35b72a2e6c669745b4af33997b2979fb";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = rainix.pkgs.${system};
      in rec {
        packages = rainix.packages.${system};

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;

          buildInputs = with pkgs;
            [ sqlx-cli cargo-expand cargo-nextest ]
            ++ rainix.devShells.${system}.default.buildInputs;
        };
      });
}
