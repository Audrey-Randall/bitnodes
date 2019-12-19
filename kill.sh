#!/bin/bash


kill $(cat .pid)
> .pid
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag > /dev/null
sudo ip link set eno1 txqueuelen 1000
ulimit -n 1024
sudo systemctl restart redis
