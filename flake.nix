{
  description = "event-sorcery: a Rust event-sourcing library on top of cqrs-es.";

  inputs = {
    rainix.url = "github:rainprotocol/rainix";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    { flake-utils, rainix, ... }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = rainix.pkgs.${system};
      in
      {
        packages = rainix.packages.${system};

        devShell = pkgs.mkShell {
          inherit (rainix.devShells.${system}.default) shellHook;
          inherit (rainix.devShells.${system}.default) nativeBuildInputs;

          buildInputs =
            with pkgs;
            [
              sqlx-cli
              cargo-expand
              cargo-nextest
              nushell
            ]
            ++ rainix.devShells.${system}.default.buildInputs;
        };
      }
    );
}
