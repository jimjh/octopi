#!/bin/bash

# GOPATH
PROJECT_PATH=$GOPATH

# Path of the config folder
CONFIG_PATH=$PROJECT_PATH/config

# Path of run folder
RUN_PATH=$PROJECT_PATH/src/octopi/run

# Path of bin folder
BIN_PATH=${PROJECT_PATH}/bin/darwin_amd64

TESTS_TOTAL=0
PASS_COUNT=0

# Build broker and place in bin
cd $RUN_PATH/broker
go clean && go build
mv broker $BIN_PATH

# Build register and place in bin
cd ../register
go clean && go build
mv register $BIN_PATH

# Build stupidproducer and place in bin
cd ../stupidproducer
go clean && go build
mv stupidproducer $BIN_PATH

# Go to bin folder. Assumes all built in here.
cd $BIN_PATH

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
	if [ $N -eq 0 ]; then
		return
	fi
	for i in `jot ${N} 1`
	do
		./broker -conf="${CONFIG_PATH}/follower${i}.json" &>/dev/null &
		FOLLOWER_PID[$i]=$!
	done
}

function clearLogs {
	cd $BIN_PATH  
        rm ../tmp/*
	cd ..
	if [ $N -eq 0 ]; then
		cd $BIN_PATH
		return
	fi
	for i in `jot ${N} 1`
	do
		rm tmp-follower${i}/*
	done
	cd $BIN_PATH
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
	if [ $N -eq 0 ]; then
		return
	fi
	for i in `jot ${N} 1`
	do
		kill ${FOLLOWER_PID[$i]}
	done
}

function cleanUp {
	killRegister
	killLeader
	killFollowers
	clearLogs
}

function testOneLeader {
	echo "Starting testOneLeader"
	TESTS_TOTAL=$((TESTS_TOTAL+1))
	N=0
	startRegister
	startLeader
	startFollowers
	# Let followers be in-sync with leader
	sleep 3
	./stupidProducer &>/dev/null
	if [ $? -eq 0 ]
	then
		PASS_COUNT=$((PASS_COUNT+1))
		echo "PASS"	
	else
		echo "FAIL"
	fi
	# Let transactions be complete
	sleep 3
	cleanUp
}

function testFollowers {
	echo "Starting testFollowers"
	TESTS_TOTAL=$((TESTS_TOTAL+1))
	N=3
	startRegister
	startLeader
	startFollowers
	sleep 3
	./stupidProducer &>/dev/null
	if [ $? -ne 0 ]
	then
		echo "FAIL"
		cleanUp
		return
	fi
	sleep 3
	killRegister
	killLeader
	killFollowers
	cd $BIN_PATH
	for i in `jot ${N} 1`
	do
		diff ../tmp/seqTopic.ocp ../tmp-follower${i}/seqTopic.ocp
		if [ $? -ne 0 ] ; then
			echo "FAIL"
			clearLogs
			return
		fi
	done
	clearLogs
	PASS_COUNT=$((PASS_COUNT+1))
	echo "PASS"
}

testOneLeader
testFollowers

echo "Passed ${PASS_COUNT}/${TESTS_TOTAL} Tests"
