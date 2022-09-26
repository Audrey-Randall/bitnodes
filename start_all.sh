#!/bin/bash

# Start bitcoind
bitcoind -daemon -datadir=/data -conf=/data/bitcoin.conf
sleep 3

# Start Bitnodes
./start.sh