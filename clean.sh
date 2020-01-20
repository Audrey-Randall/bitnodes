#!/bin/bash

rm log/*f9beb4d9*

if [ "$1" == "--hard" ]; then 
    rm data/crawl/f9beb4d9/*.json data/crawl/f9beb4d9/*.csv
fi
