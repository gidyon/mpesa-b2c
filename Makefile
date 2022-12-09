API_IN_PATH := api/proto
API_OUT_PATH := pkg/api
OPEN_API_V2_OUT_PATH := api/openapiv2

setup_dev: ## Sets up a development environment for the emrs project
	@cd deployments/compose/dev &&\
	docker-compose up -d

setup_redis:
	@cd deployments/compose/dev &&\
	docker-compose up -d redis

teardown_dev: ## Tear down development environment for the emrs project
	@cd deployments/compose/dev &&\
	docker-compose down

protoc_mpesa_b2c.v1:
	@protoc -I=$(API_IN_PATH) -I=third_party --go-grpc_out=$(API_OUT_PATH)/b2c/v1 --go-grpc_opt=paths=source_relative --go_out=$(API_OUT_PATH)/b2c/v1 --go_opt=paths=source_relative b2c.v1.proto
	@protoc -I=$(API_IN_PATH) -I=third_party --grpc-gateway_out=logtostderr=true,paths=source_relative:$(API_OUT_PATH)/b2c/v1 b2c.v1.proto
	@protoc -I=$(API_IN_PATH) -I=third_party --openapiv2_out=logtostderr=true,repeated_path_param_separator=ssv:$(OPEN_API_V2_OUT_PATH) b2c.v1.proto

copy_documentation:

protoc_all: protoc_mpesa_b2c.v1 copy_documentation

help: ## Display this help screen
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
