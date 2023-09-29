#!/bin/bash

git submodule update --init --recursive
cd external/clht
make clean clht_lb