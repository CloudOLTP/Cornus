#!/bin/bash
# setup library path
# $1 - home directory
cd /etc/ld.so.conf.d
echo "$1/libs/" | sudo tee -a other.conf
echo "/usr/local/lib" | sudo tee -a other.conf
echo "/usr/lib/x86_64-linux-gnu/" | sudo tee -a other.conf
sudo /sbin/ldconfig
