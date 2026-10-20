# JOSE 1.11.12 treats warnings as errors; OTP 29 deprecates old catch syntax.
# Suppress only that upstream compiler warning, not project checks.
export ERL_COMPILER_OPTIONS = [nowarn_deprecated_catch]

.PHONY: setup prepare check test run format smoke

setup:
	gleam deps download
	python3 scripts/patch-mist.py

prepare:
	python3 scripts/patch-mist.py

check: prepare
	gleam format --check src test
	gleam check
	gleam test

test: prepare
	gleam test

run: prepare
	gleam run

format:
	gleam format src test

smoke: prepare
	node examples/smoke.mjs

.PHONY: smoke-s3 s3-conformance s3-test-server
smoke-s3: prepare
	node examples/s3-smoke.mjs

s3-conformance: prepare
	gleam run -m highway/storage/conformance

s3-test-server:
	python3 scripts/s3-test-server.py
