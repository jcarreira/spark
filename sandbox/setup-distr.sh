#!/bin/bash
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

../sbin/start-master.sh
../sbin/slaves.sh mkdir -p ${SRC_DIR}
../sbin/slaves.sh rsync -qave ssh $(hostname):${SRC_DIR}/ ${SRC_DIR}/
../sbin/start-slaves.sh
