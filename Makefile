
.PHONY: build-manager
build-manager:
	$(MAKE) -C manager docker-build IMG=mattison/okk-manager:0.1.0


.PHONY: proto-manager
proto-manager:
	cd proto && \
	protoc \
		--go_out=../manager/internal/proto \
		--go_opt paths=source_relative \
		--plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
		--go-grpc_out=../manager/internal/proto \
		--go-grpc_opt paths=source_relative \
		--plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
		--go-vtproto_out=../manager/internal/proto \
		--go-vtproto_opt paths=source_relative \
		--plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
		--go-vtproto_opt=features=marshal+unmarshal+unmarshal_unsafe+size+pool+equal+clone \
		*.proto