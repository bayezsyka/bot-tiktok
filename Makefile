.PHONY: install dev test lint typecheck migrate create-admin clean

VENV = .venv/bin/

install:
	$(VENV)pip install -r requirements-dev.txt

dev:
	$(VENV)uvicorn app.main:app --host 0.0.0.0 --port 3200 --reload

test:
	$(VENV)pytest -v

lint:
	$(VENV)ruff check .

typecheck:
	$(VENV)mypy app

migrate:
	$(VENV)python cli.py init-db

create-admin:
	$(VENV)python cli.py create-admin

clean:
	$(VENV)python cli.py prune-temp --max-age-hours 0
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
