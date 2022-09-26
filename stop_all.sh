#!/bin/bash

# Shut down bitcoind
bitcoin-cli -conf=/data/bitcoin.conf stop
# Remove debug log
rm /data/debug.log
# Kill old bitnodes
pkill python3


