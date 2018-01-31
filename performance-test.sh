#!/bin/bash

cd "$(dirname $0)"

while :; do
    python -m performance_test || break
done
