# Makefile for ham-orm development

.PHONY: help install install-dev test test-verbose coverage clean lint format check

# Default target
help:
	@echo "Available commands:"
	@echo "  install     - Install the package"
	@echo "  install-dev - Install package in development mode with test dependencies"
	@echo "  test        - Run tests"
	@echo "  test-verbose- Run tests with verbose output"
	@echo "  coverage    - Run tests with coverage report"
	@echo "  lint        - Run linting checks"
	@echo "  format      - Format code"
	@echo "  check       - Run all checks (lint + test)"
	@echo "  clean       - Clean build artifacts"

# Install package
install:
	pip install .

# Install in development mode with test dependencies  
install-dev:
	pip install -e ".[test]"

# Run tests
test:
	pytest

# Run tests with verbose output
test-verbose:
	pytest -v

# Run tests with coverage
coverage:
	pytest --cov=src/ham_orm --cov-report=html --cov-report=term

# Clean build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Run linting (if you want to add linting tools later)
lint:
	@echo "Linting not configured yet. Consider adding flake8, black, isort, etc."

# Format code (if you want to add formatting tools later)  
format:
	@echo "Formatting not configured yet. Consider adding black, isort, etc."

# Run all checks
check: test
	@echo "All checks completed!"
