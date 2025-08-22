#!/bin/bash

git submodule update --init --recursive


# Install Python dependencies
python3 -m venv ./venv
./venv/bin/pip3 install pandas matplotlib numpy tabulate jinja2

# Setup external dependencies
cd external/abseil-cpp
git checkout linear-probe
cd ../../
cd external/libcuckoo
git checkout get_size
cd ../../
cd external/clht
make dependencies clht_lb
cd ../../
