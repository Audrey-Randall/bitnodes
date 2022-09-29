#!/bin/bash

# Copy the debug log so I can see what went wrong
echo "Copying log file"
# pwd
# hostname
cp /data/debug.log tmp_debug.log
# sleep 5

# Shut down bitcoind
bitcoin-cli -conf=/data/bitcoin.conf stop
sleep 10

# Restart bitcoind
rm /data/debug.log
bitcoind -daemon -datadir=/data -conf=/data/bitcoin.conf

# Kill old bitnodes
pkill python3


