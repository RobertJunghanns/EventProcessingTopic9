# EventProcessingTopic9

## Prerequisites

- Docker
- docker-compose

## Run the App

I recommend to not try and run this locally, PySiddhi is a absolute nightmare!

- generate event interval files with `make generate-event-files`
- generate the docker config with `make config`
- run the docker compose bundle with `make run`
- test the AND(E, SEQ(J, A)) implementation with node 4 and 9 with  `make statement-test-1`
- test the AND(C, E, D, F) implementation with node 2 with  `make statement-test-2`
- test the AND(E, SEQ(C, J, A)) implementation with node 4 and 9 with  `make statement-test-3`

## Tests

`make test`
