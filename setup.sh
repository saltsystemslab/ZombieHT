#!/bin/bash

git submodule update --init --recursive

cd external/abseil-cpp
git checkout linear-probe
cd ../../
cd external/libcuckoo
git checkout get_size
cd ../../
cd external/clht
make dependencies clht_lb
cd ../../
