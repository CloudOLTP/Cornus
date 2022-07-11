#!/bin/bash

source $HOME/Sundial-Private/tools/setup_env.sh
dirname="$HOME/Sundial-Private/cornus_storage/proto"
fname="sundial"
echo "directory of proto: ${dirname}"
protoc -I=${dirname} --grpc_out=${dirname} --plugin=protoc-gen-grpc=`which grpc_cpp_plugin` ${dirname}/${fname}.proto
protoc -I=${dirname} --cpp_out=${dirname} ${dirname}/${fname}.proto
cd ${dirname}
mv ${fname}.grpc.pb.cc ${fname}.grpc.pb.cpp
mv ${fname}.pb.cc ${fname}.pb.cpp
