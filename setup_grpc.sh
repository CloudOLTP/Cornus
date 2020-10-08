sudo apt-get install build-essential autoconf libtool pkg-config
sudo apt-get install cmake
sudo apt-get install libgflags-dev
sudo apt-get install clang-5.0 libc++-dev
cd ..
git clone https://github.com/grpc/grpc
cd grpc
git submodule update --init
cd test/distrib/cpp/
sudo ./run_distrib_test_cmake.sh
export PKG_CONFIG_PATH=/usr/local/grpc/lib/pkgconfig
export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH
