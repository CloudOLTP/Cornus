cd /etc/ld.so.conf.d
echo "/users/LockeZ/Sundial/libs/" | sudo tee other.conf
echo "/usr/local/lib" | sudo tee other.conf
sudo /sbin/ldconfig
