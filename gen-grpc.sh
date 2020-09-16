#!/bin/bash
protoc -I . zahif.proto --go_out=plugins=grpc:./internal/zahif/proto
