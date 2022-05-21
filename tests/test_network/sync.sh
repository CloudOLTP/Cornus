make clean;
for i in 6 7 8
do
rsync -avh --exclude 'proto' --delete /home/kanwu/Sundial/test_network/ kanwu@compute${i}:/home/kanwu/Sundial/test_network
done
