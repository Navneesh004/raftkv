.PHONY: build test race stress bench up down chaos fmt

build:
	go build ./...

test:
	go test -count=1 ./...

race:
	go test -race -count=1 ./...

# Every Raft test 50 times with the race detector (slow: ~3 hours).
stress:
	go test -race -count=50 -timeout 0 ./raft/

bench:
	go run ./cmd/raftkv-bench -mode all -out bench/results

up:
	docker compose -f deploy/docker-compose.yml up -d --build

down:
	docker compose -f deploy/docker-compose.yml down -v

chaos:
	./deploy/chaos.sh 3600 8

fmt:
	gofmt -w .
