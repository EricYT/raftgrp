#!/bin/bash

# for debuging
#set -x

RAFT_EXAMPLE_BIN=./raftgrpexample
RAFT_RPC_PORT_BASE=9527
PORTAL_RPC_PORT_BASE=9021
PPROF_PORT_BASE=6060

function Usage {
  echo "Usage: $0 TotalNodes NodeID"
  echo "  Example: $0 3 1"
  exit 1
}

function isNumber {
  re='^[1-9]+$'
  if [[ "$1" =~ $re ]];then
    return 0
  fi
  return 1
}

function makeCluster {
  local nodeCount=$1
  local cluster=""
  for i in $(seq 1 ${nodeCount});do
    local raftRpcPort=`expr $RAFT_RPC_PORT_BASE + $i - 1`
    if [ -z "$cluster" ];then
      cluster=${i}=127.0.0.1:${raftRpcPort}
    else
      cluster=${cluster},${i}=127.0.0.1:${raftRpcPort}
    fi
  done
  echo cluster
}

function StartN {
  # Validation arguments
  isNumber $1
  if [ $? -ne 0 ];then
    echo "node count is not integer"
    Usage
  fi
  local nodeCount=$1

  isNumber $2
  if [ $? -ne 0 ];then
    echo "node id is not integer"
    Usage
  fi
  local nodeID=$2

  if [ $nodeCount -lt $nodeID ];then
    echo "node count is lesser than node id"
    Usage
  fi

  echo "node count ${nodeCount} current node id ${nodeID}"

  # generate cluster param
  local cluster=""
  for i in $(seq 1 ${nodeCount});do
    local raftRpcPort=`expr $RAFT_RPC_PORT_BASE + $i - 1`
    if [ -z "$cluster" ];then
      cluster=${i}=127.0.0.1:${raftRpcPort}
    else
      cluster=${cluster},${i}=127.0.0.1:${raftRpcPort}
    fi
  done
  echo "cluster: ${cluster}"

  local raftRpcPort=`expr ${RAFT_RPC_PORT_BASE} + $2 - 1`
  local portalRpcPort=`expr ${PORTAL_RPC_PORT_BASE} + $2 - 1`
  local pprofPort=`expr ${PPROF_PORT_BASE} + $2 - 1`
  echo "start node ${1}"
  $RAFT_EXAMPLE_BIN -pprof-port ${pprofPort} -gid 1 -id ${nodeID} -cluster "${cluster}" -grpc-addr "127.0.0.1:${raftRpcPort}" -port ${portalRpcPort} -new-cluster true 2>&1 | tee node${nodeID}.log
}

[[ $# -lt 2 ]] && Usage

if [ ! -e "$RAFT_EXAMPLE_BIN" ];then
  echo "Failed: raft example binary(${RAFT_EXAMPLE_BIN}) not found"
  exit 2
fi

StartN "$@"

exit 0
