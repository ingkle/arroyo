#!/bin/env bash

set -e

NEXUS_FILES="http://192.168.200.11:8081/repository/offline-files/arroyo"

# Install arch specific build dependencies
echo env
if [ "$(uname -m)" = "x86_64" ]; then
  export PROTO_ARCH="x86_64";
  export MOLD_ARCH="x86_64";
  export PY_ARCH="x86_64";
elif [ "$(uname -m)" = "aarch64" ]; then
  export PROTO_ARCH="aarch_64";
  export MOLD_ARCH="aarch64";
  export PY_ARCH="aarch64";
else
  echo "Unsupported architecture: $(uname -m)"
  exit 1;
fi

# Install Python 3.12
curl -OL ${NEXUS_FILES}/cpython-3.12.5-${PY_ARCH}-linux.tar.gz
tar xvfz cpython*.tar.gz
cp -r python/bin/* /usr/local/bin/
cp -r python/include/* /usr/local/include/
cp -r python/lib/* /usr/local/lib/
cp -r python/share/* /usr/local/share/
ldconfig

# Install mold
curl -OL ${NEXUS_FILES}/mold-1.11.0-${MOLD_ARCH}-linux.tar.gz
tar xvfz mold*.tar.gz
mv mold*-linux/bin/* /usr/bin
mv mold*-linux/libexec/* /usr/libexec
rm -rf mold*

# Install protoc
curl -OL ${NEXUS_FILES}/protoc-21.8-linux-${PROTO_ARCH}.zip
unzip protoc*.zip
mv bin/protoc /usr/local/bin
rm -rf protoc*.zip
