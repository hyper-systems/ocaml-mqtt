let
  onix = import (builtins.fetchGit {
    url = "https://github.com/rizo/onix.git";
    rev = "1a67fe51d4d1676f38088e635a00dfdae5bae70b";
  }) { verbosity = "info"; };

in onix.env {
  path = ./.;
  vars = {
    "with-test" = true;
    "with-doc" = true;
    "with-dev-setup" = true;
  };
  deps = { "ocaml-base-compiler" = "5.1.0"; };
}
