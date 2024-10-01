run:
	@echo "Starting application..."
	@go run cmd/JacuteSQL/main.go --config config/config.yaml
build:
	@echo "Building application..."
	@go build cmd/JacuteSQL/main.go
