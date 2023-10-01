#!/bin/bash

git submodule update --init --recursive
cd external/clht
./scripts/make_dependencies.sh
make clean clht_lb
