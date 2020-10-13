cd /etc/ld.so.conf.d
echo "/users/LockeZ/Sundial/libs/" | sudo tee -a other.conf
echo "/usr/local/lib" | sudo tee -a other.conf
sudo /sbin/ldconfig
