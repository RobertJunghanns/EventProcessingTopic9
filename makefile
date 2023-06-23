.PHONY: test
test:
	@echo "Running tests..."
	@docker-compose run --build main pytest tests.py