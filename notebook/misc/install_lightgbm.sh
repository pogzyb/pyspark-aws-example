#!/usr/bin/env bash

cd /root
wget http://files1.directadmin.com/services/custombuild/cmake-2.8.11.tar.gz
tar xzf cmake-2.8.11.tar.gz
cd cmake-2.8.11
./configure
make
make install

cd /home/jovyan

git clone --recursive https://github.com/microsoft/LightGBM ; cd LightGBM
mkdir build ; cd build
cmake ..
make -j4
