#!/bin/bash
SRC_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." && pwd )"

echo ../sbin/start-master.sh
echo ../sbin/slaves.sh mkdir -p ${SRC_DIR}
echo ../sbin/slaves.sh rsync -qave ssh $(hostname):${SRC_DIR}/ ${SRC_DIR}/
echo ../sbin/start-slaves.sh
