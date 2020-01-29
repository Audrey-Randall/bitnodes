#!/bin/bash
# --- bitcoin mainnet: f9beb4d9 (db = 0) ---
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
echo never | sudo tee /sys/kernel/mm/transparent_hugepage/defrag > /dev/null
sudo systemctl restart redis
sudo ip link set eno1 txqueuelen 5000
ulimit -n 1000000

python -u crawl.py conf/crawl.f9beb4d9.conf master > log/crawl.f9beb4d9.master.out 2>&1 &
echo $! > .pid
python -u crawl.py conf/crawl.f9beb4d9.conf slave > log/crawl.f9beb4d9.slave.1.out 2>&1 &
echo $! >> .pid
python -u crawl.py conf/crawl.f9beb4d9.conf slave > log/crawl.f9beb4d9.slave.2.out 2>&1 &
echo $! >> .pid

python -u ping.py conf/ping.f9beb4d9.conf master > log/ping.f9beb4d9.master.out 2>&1 &
echo $! >> .pid
python -u ping.py conf/ping.f9beb4d9.conf slave > log/ping.f9beb4d9.slave.1.out 2>&1 &
echo $! >> .pid
python -u ping.py conf/ping.f9beb4d9.conf slave > log/ping.f9beb4d9.slave.2.out 2>&1 &
echo $! >> .pid
python -u ping.py conf/ping.f9beb4d9.conf slave > log/ping.f9beb4d9.slave.3.out 2>&1 &
echo $! >> .pid
python -u ping.py conf/ping.f9beb4d9.conf slave > log/ping.f9beb4d9.slave.4.out 2>&1 &
echo $! >> .pid
python -u ping.py conf/ping.f9beb4d9.conf slave > log/ping.f9beb4d9.slave.5.out 2>&1 &
echo $! >> .pid
python -u ping.py conf/ping.f9beb4d9.conf slave > log/ping.f9beb4d9.slave.6.out 2>&1 &
echo $! >> .pid

python -u resolve.py conf/resolve.f9beb4d9.conf > log/resolve.f9beb4d9.out 2>&1 &
echo $! >> .pid

python -u export.py conf/export.f9beb4d9.conf > log/export.f9beb4d9.out 2>&1 &
echo $! >> .pid

#python -u seeder.py conf/seeder.f9beb4d9.conf > log/seeder.f9beb4d9.out 2>&1 &

#python -u pcap.py conf/pcap.f9beb4d9.conf > log/pcap.f9beb4d9.1.out 2>&1 &
#python -u pcap.py conf/pcap.f9beb4d9.conf > log/pcap.f9beb4d9.2.out 2>&1 &
#python -u pcap.py conf/pcap.f9beb4d9.conf > log/pcap.f9beb4d9.3.out 2>&1 &
