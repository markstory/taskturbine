# Taskturbine Python

Python SDK for taskturbine. Built on top of the taskturbine-core package in this repository.

## Development setup

1. Choose a supported python (python3.13) `pyenv local 3.13.5`
2. Run `python -m venv .venv`
3. Run `source .venv/bin/activate`
4. Run `uv sync`

## Building

1. Run `cargo build`
2. Run `maturin develop` or `maturin develop --release`
3. Use `python` to run scripts that can import the built module.
