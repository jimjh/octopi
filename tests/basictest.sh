#!/bin/bash

# GOPATH
PROJECT_PATH=$GOPATH

# Path of the config folder
CONFIG_PATH=$PROJECT_PATH/config

# Number of followers
N=3

# Go to the bin path. Assumes all built in here.
cd ${PROJECT_PATH}/bin/darwin_amd64

# startRegister starts the register in the background
function startRegister {
	./register -conf="${CONFIG_PATH}/reg.json" &>/dev/null &
	REG_PID=$!
	sleep 5
}

# startLeader starts the leader in the background
function startLeader {
	./broker -conf="${CONFIG_PATH}/leader.json" &>/dev/null &
	LEADER_PID=$!
	sleep 5
}

# startFollowers starts the followers in the background
function startFollowers {
	for i in `jot ${N} 1`
	do
		./broker -conf="${CONFIG_PATH}/follower${i}.json" &>/dev/null &
		FOLLOWER_PID[$i]=$!
	done
}

# killRegister kills the register
function killRegister {	
	kill ${REG_PID}
}

# killLeader kills the leader
function killLeader {
	kill ${LEADER_PID}
}

# killFollowers kills the followers
function killFollowers {
	for i in `jot ${N} 1`
	do
		kill ${FOLLOWER_PID[$i]}
	done
}

function testStupidProducer {
	./stupidproducer
	echo $?
}

startRegister
startLeader
startFollowers
testStupidProducer
killRegister
killLeader
killFollowers
