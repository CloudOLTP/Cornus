make clean
for i in $1
do
	rsync -av --exclude 'proto' --exclude 'outputs' --delete /home/kanwu/Sundial/ kanwu@compute${i}:/home/kanwu/Sundial/
done
