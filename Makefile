SHELL := /bin/bash
PWD := $(shell pwd)

default: docker-image

all:

docker-image:
	docker build -f ./input_boundary/Dockerfile -t "input_boundary:latest" .
	docker build -f ./output_boundary/Dockerfile -t "output_boundary:latest" .
	docker build -f ./client/Dockerfile -t "client:latest" .
	# docker build -f ./book_filter/Dockerfile -t "book_filter:latest" .
	# docker build -f ./review_filter/Dockerfile -t "review_filter:latest" .
	# docker build -f ./author_decades_counter/Dockerfile -t "author_decades_counter:latest" .
	# docker build -f ./router/Dockerfile -t "router:latest" .
	# docker build -f ./review_stats_service/Dockerfile -t "review_stats_service:latest" .
	# docker build -f ./sentiment_analyzer/Dockerfile -t "sentiment_analyzer:latest" .
	# docker build -f ./sentiment_aggregator/Dockerfile -t "sentiment_aggregator:latest" .
	# docker build -f ./review_mean_aggregator/Dockerfile -t "review_mean_aggregator:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't leave uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-compose-up: docker-image
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-down:
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: docker-compose-down

docker-compose-logs:
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs

docker-compose-up-gen: docker-image
	# python3 ./docker-compose-generator/main.py
	docker compose -f docker-compose-gen.yaml up -d --build
.PHONY: docker-compose-up-gen

docker-compose-down-gen:
	docker compose -f docker-compose-gen.yaml stop -t 2
	docker compose -f docker-compose-gen.yaml down
.PHONY: docker-compose-down-gen

docker-compose-logs-gen:
	docker compose -f docker-compose-gen.yaml logs -f
.PHONY: docker-compose-logs-gen

rabbitmq-up:
	docker compose -f docker-compose-rabbit.yaml up -d
.PHONY: rabbitmq-up

rabbitmq-down:
	docker compose -f docker-compose-rabbit.yaml down
.PHONY: rabbitmq-down