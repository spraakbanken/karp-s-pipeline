
.PHONY: help
help:
	@echo "usage:"
	@echo ""
	@echo "install"
	@echo "fmt"
	@echo "test"

.PHONY: install
install:
	pip install --upgrade pip
	pip install -e .[dev]

.PHONY: fmt
fmt:
	ruff format .
	ruff check --fix . 

.PHONY: test
test:
	pytest
