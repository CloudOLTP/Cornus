#!/bin/bash

# dependencies
sudo apt-get -y install build-essential autoconf libtool pkg-config
sudo apt-get -y install cmake
sudo apt-get -y install libgflags-dev
sudo apt-get -y install clang-5.0 libc++-dev
sudo apt update
sudo apt -y install python3-pip
pip3 install pandas

# environment setup
export MY_INSTALL_DIR=$HOME/.local
mkdir -p $MY_INSTALL_DIR
export PATH="$PATH:$MY_INSTALL_DIR/bin"

# update cmake
wget -q -O cmake-linux.sh https://github.com/Kitware/CMake/releases/download/v3.19.6/cmake-3.19.6-Linux-x86_64.sh
sudo sh cmake-linux.sh -- --skip-license --prefix=$MY_INSTALL_DIR
rm cmake-linux.sh

# install grpc
cd ~/
git clone --recurse-submodules -b v1.46.3 --depth 1 --shallow-submodules https://github.com/grpc/grpc
cd grpc
mkdir -p cmake/build
pushd cmake/build
cmake -DgRPC_INSTALL=ON \
      -DgRPC_BUILD_TESTS=OFF \
      -DCMAKE_INSTALL_PREFIX=$MY_INSTALL_DIR \
      ../..
make -j
sudo make install
popd

#git clone https://github.com/grpc/grpc
#cd grpc
#git submodule update --init
#cd test/distrib/cpp/
#sudo ./run_distrib_test_cmake.sh
#export PKG_CONFIG_PATH=/usr/local/grpc/lib/pkgconfig
#export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
