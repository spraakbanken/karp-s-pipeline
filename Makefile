
.PHONY: help
help:
	@echo "usage:"
	@echo ""
	@echo "install"
	@echo "fmt"
	@echo "test"

.PHONY: install
install:
	uv sync

.PHONY: fmt
fmt:
	ruff format .
	ruff check --fix . 

.PHONY: test
test:
	pytest

.PHONY: check-types
check-types:
	basedpyright

