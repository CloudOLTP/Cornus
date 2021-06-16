cd /etc/ld.so.conf.d
#echo "/users/scarletg/Sundial/libs/" | sudo tee -a other.conf
echo "/home/kanwu/Sundial/libs/" | sudo tee -a other.conf
echo "/usr/local/lib" | sudo tee -a other.conf
echo "/usr/lib/x86_64-linux-gnu/" | sudo tee -a other.conf
sudo /sbin/ldconfig
