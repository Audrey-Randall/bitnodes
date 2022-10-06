#!/bin/bash

blockheight=`bitcoin-cli -conf=/data/bitcoin.conf getblockcount`

for i in {1..10}
do
   bitcoin-cli -conf=/data/bitcoin.conf getblockhash $((blockheight-i))
done
