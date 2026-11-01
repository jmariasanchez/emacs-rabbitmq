CARGO ?= cargo
EMACS ?= emacs
ERT_SELECTOR ?= (not (tag :integration))

.PHONY: build test ert bytecompile check

build:
	$(CARGO) build --workspace --locked

test:
	$(CARGO) test --workspace --locked

ert:
	$(EMACS) --batch -Q -L lisp -L tests/ert \
		--eval '(setq rabbitmq-bridge-program (or (getenv "RABBITMQ_BRIDGE") (expand-file-name "target/debug/rabbitmq-bridge")))' \
		-l rabbitmq-test \
		--eval "(ert-run-tests-batch-and-exit '$(ERT_SELECTOR))"

bytecompile:
	$(EMACS) --batch -Q -L lisp -f batch-byte-compile lisp/*.el

check: build test bytecompile ert
