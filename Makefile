.DEFAULT_GOAL := build

.PHONY: build
build:
	dune build --root=.

.PHONY: rebuild
rebuild:
	dune build --root=. -w

.PHONY: test
test:
	dune build --root=. @runtest

.PHONY: retest
retest:
	dune build --root=. -w @runtest

.PHONY: doc
doc:
	dune build --root=. @doc

.PHONY: redoc
redoc:
	dune build --root=. -w @doc

.PHONY: clean
clean:
	dune clean --root=.
