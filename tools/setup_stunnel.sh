sudo apt-get update
sudo apt-get install -y stunnel4
sudo sed -i 's/ENABLED=0/ENABLED=1/g' /etc/default/stunnel4
echo "[redis-cli]" | sudo tee /etc/stunnel/redis.conf
echo "client = yes" | sudo tee -a /etc/stunnel/redis.conf
echo "accept = 127.0.0.1:6380" | sudo tee -a /etc/stunnel/redis.conf
echo "connect = cornusredis.redis.cache.windows.net:6380" | sudo tee -a /etc/stunnel/redis.conf
