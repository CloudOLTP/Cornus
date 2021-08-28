#!/bin/bash
make clean

read -p 'sync from node 2 to which node? ' num
i=1
while (( ++i <= num )); do
	rsync -av --exclude 'proto' --exclude 'outputs' --delete /home/kanwu/Sundial/ kanwu@compute${i}:/home/kanwu/Sundial/
done
