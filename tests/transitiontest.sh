#!/bin/bash

# GOPATH
PROJECT_PATH=$GOPATH

# Path of the config folder
CONFIG_PATH=$PROJECT_PATH/config

# Path of run folder
RUN_PATH=$PROJECT_PATH/src/octopi/run

# Path of bin folder
BIN_PATH=${PROJECT_PATH}/bin/darwin_amd64
TEST_PATH=${PROJECT_PATH}/../tests
TMP_PATH=${PROJECT_PATH}/bin/tmp

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

. ${TEST_PATH}/helper.sh

function testSimpleTransition {
	echo "Starting testSimpleTransition"
	TESTS_TOTAL=$((TESTS_TOTAL+1))
	N=3
	NSTART=3
	startRegister
	startLeader
	startFollowers
	sleep 3
	killLeader
	sleep 3
	./stupidproducer &>/dev/null &
	sleep 5
	checkFollowerLogs
	passFail $?
	killRegister
	killFollowers
	clearLogs
}

function testTransitionAdd {
    echo "Starting testTransitionAdd..."
    TESTS_TOTAL=$((TESTS_TOTAL+1))
    N=1
	NSTART=3
    startRegister
    startLeader
    startFollowers
    sleep 3
    killLeader
    sleep 2
    startFollower 2
	startFollower 3
    ./stupidproducer &>/dev/null &
    sleep 3
    checkFollowerLogs
    passFail $?
    killRegister
    killFollowers
    #clearLogs
}

function testRandomTransition {
	echo "Starting testRandomTransition..."
	TESTS_TOTAL=$((TESTS_TOTAL+1))
	NSTART=3
	N=3
	M=20
	startRegister
	startLeader
	sleep 2
	startFollowers
	sleep 3
	killLeader
	sleep 2
	for i in `jot ${M} 1`
        do
                ./stupidproducer -id="Producer${i}" &>/dev/null &
		randNum=$(((RANDOM % $NSTART)+1))
                if [ $N -gt 1 ]
		then
			killOrStart $randNum
		else
			startFollower $randNum
		fi
                sleep 8
        done

	for i in `jot ${NSTART} 1`
	do
		startFollower $i
	done
	sleep 20
	checkFollowerLogs
	passFail $?
	killRegister
	killFollowers
	#clearLogs
}

clearLogs
#testSimpleTransition
#testTransitionAdd
testRandomTransition

echo "Passed ${PASS_COUNT}/${TESTS_TOTAL} Tests"
