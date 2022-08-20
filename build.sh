#!/bin/bash

# 执行时显示指令及参数
set -x

SOURCE_DIR=`pwd`
BUILD_DIR=${SOURCE_DIR:-/build}
BUILD_TYPE=${BUILD_TYPE:-release}
CXX={CXX:-g++}

# for clang-tidy
ln -sf $BUILD_DIR/$BUILD_TYPE-cpp17/compile_commands.json

mkdir -p $BUILD_DIR/$BUILD_TYPE-cpp17 \
  && cd $BUILD_DIR/$BUILD_TYPE-cpp17 \
  && cmake \
           -DCMAKE_BUILD_TYPE=$BUILD_TYPE \
           -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
           $SOURCE_DIR \
  && make $*
