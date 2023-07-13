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

.PHONY: statement-test-1
statement-test-1:
	@echo "Running statement test 1..."
	@docker-compose -f docker-compose.statements.test1.yaml up --build

.PHONY: statement-test-2
statement-test-2:
	@echo "Running statement test 2..."
	@docker-compose -f docker-compose.statements.test2.yaml up --build

.PHONY: generate-event-files
generate-event-files:
	@echo "Running Event File Generator..."
	@python src/eventIntervalGenerator.py