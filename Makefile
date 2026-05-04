.PHONY: up down logs redis-cli build tidy

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f

redis-cli:
	docker compose exec redis redis-cli

build:
	go build ./producer ./consumer ./dashboard

tidy:
	go mod tidy