.PHONY: test
test:
	@echo "Running tests..."
	@docker-compose -f docker-compose.test.yaml  up --build

.PHONY: config
config:
	@echo "Running config..."
	@python src/compose_from_statements.py

.PHONY: run
run:
	@echo "Running..."
	@docker-compose -f docker-compose.statements.yaml up --build