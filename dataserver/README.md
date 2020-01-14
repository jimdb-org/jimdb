JIMDB Dataserver
-----------------------------------

# Prerequisite
- c++ complier with c++11 support
- cmake version 3.8+
- curl library and developemnt files   

``` 
# centos
yum install -y libcurl libcurl-devel
# ubuntu
sudo apt install libcurl4 libcurl4-openssl-dev
``` 

# build
```sh
mkdir build
cd build
cmake ..
make -j `nproc`
```

# build and run tests
```sh
mkdir build
cd build
cmake .. -DBUILD_TEST=ON
make -j `nproc`
ctest
```
