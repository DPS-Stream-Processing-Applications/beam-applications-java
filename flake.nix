{
  description = "Apache Beam applications to be run with Apache Flink";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
  };

  outputs = { nixpkgs, ... }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };

    in
    {
      devShells.${system} = {
        default = pkgs.mkShell {
          buildInputs = with pkgs; [
            jdk11
            flink

            kubectl
            k3d
            kubernetes-helm
          ];

          /* INFO:
           * The flink package from the nix store contains the helper scripts
           * inside the `opt` folder. To make these scripts available the folder
           * gets addet do `PATH`.
           */
          shellHook = ''
            export PATH=$PATH:${pkgs.flink}/opt/flink/bin
            export PS1="(nix-shell) $PS1" # NOTE: To communicate that a nix shell is active
          '';
        };
      };
    };
}
