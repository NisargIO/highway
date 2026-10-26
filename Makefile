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
	node --test sdk/javascript/test/*.test.mjs

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

.PHONY: release sdk-test smoke-release image
release: prepare
	gleam export erlang-shipment

sdk-test:
	node --test sdk/javascript/test/*.test.mjs

smoke-release: release
	node examples/release-smoke.mjs

image:
	docker build -t highway:0.1.0 .

.PHONY: smoke-cluster
smoke-cluster: release
	node examples/cluster-smoke.mjs

.PHONY: sdk-check smoke-container benchmark
sdk-check:
	npm --prefix sdk/javascript ci --ignore-scripts
	npm --prefix sdk/javascript run check
	npm --prefix sdk/javascript test

smoke-container: image
	node examples/container-smoke.mjs

benchmark: smoke-release

.PHONY: proxy-check
proxy-check:
	node examples/proxy-smoke.mjs

.PHONY: compose-config compose-up compose-down compose-token compose-smoke
compose-config:
	node scripts/compose-config.mjs

compose-up: compose-config
	docker compose up --build -d --wait

compose-down:
	docker compose down

compose-token:
	node scripts/compose-config.mjs token

compose-smoke: compose-up
	node examples/compose-smoke.mjs

.PHONY: smoke-scoped
smoke-scoped: release
	node examples/scoped-smoke.mjs
