sudo apt-get update
sudo apt-get install -y software-properties-common
sudo apt-get install -y python-software-properties
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key | sudo apt-key add -
sudo add-apt-repository -y 'deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-4.0 main'
sudo apt-get update
sudo apt-get install -y build-essential gcc g++ clang lldb lld gdb cmake git protobuf-compiler libprotobuf-dev flex bison libnuma-dev
sudo apt-get install -y dstat
sudo apt-get install -y vim htop 
sudo apt-get install -y vagrant cmake curl
sudo apt install -y libjemalloc-dev
sudo apt install -y openjdk-8-jre-headless
sudo apt install -y cgroup-tools
sudo apt install -y python3-pip
sudo apt install -y numactl
pip3 install --upgrade pip
pip3 install pandas
echo "set tabstop=4" > ~/.vimrc
git config --global user.name "sherlockwu"
git config --global user.email "wukanustc@gmail.com"

(echo ; echo ; echo ; echo ; echo ; echo ;echo ; echo ; echo ; echo ;) | ssh-keygen -t rsa -b 4096 -C "kwu54@wisc.edu"
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub

git clone https://github.com/redis/redis.git
cd redis
make

cd
mkdir redis_data/
#sudo make install






