protofiles = v1/*/*.proto

gen-all: gen-dir gen-grpc

gen-dir:
	@mkdir -p gen

## compile all proto file
gen-grpc:
	@for proto in $(protofiles) ; do \
  		protoc $$proto -I=. \
  		--go_out=gen --go_opt=paths=source_relative \
  		--go-grpc_out=gen --go-grpc_opt=paths=source_relative; \
    done

deps:
	go install google.golang.org/protobuf/cmd/protoc-gen-go
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc