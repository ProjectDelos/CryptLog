# Install docker on the local machine

bench: xcompile awsbench

awsbench:
	go build ./awsbench

xcompile: proxy xcompiler server-ex client-ex

proxy:
	env GOOS=linux GOARCH=amd64 go build ./dynamodb/proxy

xcompiler:
	docker build -t xcompiler ./builder

server:
	eval "$(docker-machine env default)"
	docker run -t -i -v $(shell pwd):/source xcompiler cargo build --release --manifest-path ./server-src/Cargo.toml
	cp ./client-src/target/release/client ./client

client:
	eval "$(docker-machine env default)"
	docker run -t -i -v $(shell pwd):/source xcompiler cargo build --release --manifest-path ./client-src/Cargo.toml
	cp ./server-src/target/release/server ./server

# dependencies
# docker
# docker-rust cross-compiler: https://github.com/Hoverbear/docker-rust
