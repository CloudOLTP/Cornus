sudo apt-get update
sudo apt-get install -y software-properties-common
sudo apt-get install -y python-software-properties
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
wget -O - http://llvm.org/apt/llvm-snapshot.gpg.key | sudo apt-key add -
sudo add-apt-repository -y 'deb http://apt.llvm.org/xenial/ llvm-toolchain-xenial-4.0 main'
sudo apt-get update
sudo apt-get install -y build-essential gcc g++ clang lldb lld gdb cmake git  flex bison libnuma-dev
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
pip3 install paramiko
echo "set tabstop=4" > ~/.vimrc

# setup git
git config --global user.name "ScarletGuo"
git config --global user.email "zguo74@wisc.edu"

# set up redis
git clone https://github.com/redis/redis.git
cd redis
make
cd
mkdir redis_data/

# set up ssh key
bash
(echo ; echo ; echo ; echo ; echo ; echo ;echo ; echo ; echo ; echo ;) | ssh-keygen -t ed25519 -C "zguo74@wisc.edu"
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
