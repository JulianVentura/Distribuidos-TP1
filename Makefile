SHELL := /bin/bash
PWD := $(shell pwd)

GIT_REMOTE = github.com/7574-sistemas-distribuidos/docker-compose-init

default: build

all:

deps:
	go mod tidy
	go mod vendor

build: deps
	GOOS=linux go build -o bin/client github.com/7574-sistemas-distribuidos/docker-compose-init/client
.PHONY: build

docker-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
	docker build -f ./client_app/Dockerfile -t "client_app:latest" .
	# Execute this command from time to time to clean up intermediate stages generated 
	# during client build (your hard drive will like this :) ). Don't left uncommented if you 
	# want to avoid rebuilding client image every time the docker-compose-up command 
	# is executed, even when client code has not changed
	# docker rmi `docker images --filter label=intermediateStageToBeDeleted=true -q`
.PHONY: docker-image

docker-server-image:
	docker build -f ./server/Dockerfile -t "server:latest" .
.PHONY: docker-server-image

docker-client-app-image:
	docker build -f ./client_app/Dockerfile -t "client_app:latest" .
.PHONY: docker-client-app-image

docker-client-query-image:
	docker build -f ./client_query/Dockerfile -t "client_query:latest" .
.PHONY: docker-client-query-image


server-up: docker-server-image
	docker-compose -f docker-compose-server.yaml up --build
.PHONY: server-up

client-app-up: docker-client-app-image
	docker-compose -f docker-compose-clients.yaml up --build
.PHONY: client-app-up

client-query-up: docker-client-query-image
	docker-compose -f docker-compose-client-query.yaml up --build
.PHONY: client-query-up

server-down:
	docker-compose -f docker-compose-server.yaml stop -t 10
	docker-compose -f docker-compose-server.yaml down
.PHONY: server-down

client-app-down:
	docker-compose -f docker-compose-clients.yaml stop -t 10
	docker-compose -f docker-compose-clients.yaml down
.PHONY: client-app-down

client-query-down:
	docker-compose -f docker-compose-client-query.yaml stop -t 10
	docker-compose -f docker-compose-client-query.yaml down
.PHONY: client-query-down

server-logs:
	docker-compose -f docker-compose-server.yaml logs -f
.PHONY: server-logs

client-app-logs:
	docker-compose -f docker-compose-clients.yaml logs -f
.PHONY: client-app-logs
