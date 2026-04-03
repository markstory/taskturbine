# Makefile
#
# Shortcuts and high level operation names for local development
# and CI. Using make for CI makes reproducing what happens in CI
# locally much simpler and forces you to avoid complex code being stored in YAML.

# Install and setup
###################

install-py: ## Install python dependencies with uv
	cd ./taskturbine-python && uv sync --all-packages --all-groups --frozen
.PHONY: install-py

install-rs: ## Install deps for rust
	cargo install
.PHONY: install-rs

# Building
###################

build-rs: install-rs ## Build cargo crates
	cargo build
.PHONY: build-rs

build-py: install-py ## Build python extension
	cd ./taskturbine-python && uv run maturin build
.PHONY: build-py


# Running tests
###################

test: test-rs test-py ## Run all tests
.PHONY: test

test-rs: ## Run rust tests
	cargo t
.PHONY: test-rs

test-py: install-py ## Run python tests
	cd ./taskturbine-python && uv run pytest

# Linting and style
###################

style: style-rs style-py ## Run style checking tools for all packages
.PHONY: style

style-rs: ## Run cargo fmt --check
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all --check
.PHONY: style-rs

style-py: install-py ## Run ruff format check on python code
	cd taskturbine-python && uv run ruff format --check .
.PHONY: style-rs

lint: lint-rs lint-py ## Run linting tools for all packages
.PHONY: lint

lint-rs: ## Run clippy for rust
	@rustup component add clippy 2> /dev/null
	cargo clippy --workspace --all-targets --all-features --no-deps --fix --allow-dirty --allow-staged -- -D warnings
.PHONY: lint-rs

lint-py: install-py
	cd taskturbine-python && uv run ruff check --fix .
.PHONY: lint-py

format: format-rs format-py ## Run style + lint autofixing for all packages
.PHONY: format

format-rs: ## Run style and lint fixing for rust (clippy, fmt)
	@rustup component add clippy 2> /dev/null
	@rustup component add rustfmt 2> /dev/null
	cargo fmt --all
	cargo clippy --workspace --all-targets --all-features --no-deps --fix --allow-dirty --allow-staged -- -D warnings
.PHONY: format-rs

format-py: install-py ## Run style fixing for py (ruff --fix)
	cd taskturbine-python && uv run ruff format .
.PHONY: format-rs


# Help
###################

help: ## this help
	@ awk 'BEGIN {FS = ":.*##"; printf "Usage: make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-10s\033[0m\t%s\n", $$1, $$2 }' $(MAKEFILE_LIST)
.PHONY: help
