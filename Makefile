.PHONY: setup check test run format

setup:
	gleam deps download

check:
	gleam format --check src test
	gleam check
	gleam test

test:
	gleam test

run:
	gleam run

format:
	gleam format src test
