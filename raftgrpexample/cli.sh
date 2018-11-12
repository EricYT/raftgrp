#!/bin/bash

#set -x

PORTAL_RPC_PORT_BASE=9021

function usage {
  echo "Usage: $0 [subcommand] arguments"
  echo "  example: $0 set 1 key 10"
  echo "    means: set 10 keies base key name is 'key' to node 1"
  exit 1
}

function setValues {
  local nodeId=$1
  local keyPrefix=$2
  local count=$3

  local portalRpcPort=`expr ${PORTAL_RPC_PORT_BASE} + ${nodeId} - 1`

  for i in $(seq 1 $count);do
    local data="value#leader#${nodeID}#index#${i}#cookie#${RANDOM}"
    ## slient way
    local cmd="curl -XPUT "http://127.0.0.1:${portalRpcPort}/${keyPrefix}${i}" -s -b "${data}""
    echo "${cmd}"
    if ! `$cmd`;then
      echo "set operation failed"
      exit 2
    fi
  done
}

[[ $# -lt 1 ]] && usage

case "$1" in
  set)
    echo "set values"
    shift
    setValues "$@"
    ;;
  *)
    echo "unknow command: $1"
    usage
esac

exit 0
