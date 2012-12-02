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
	if [ $N -eq 0 ]
	then
		return
	fi
	for i in `jot ${N} 1`
	do
		./broker -conf="${CONFIG_PATH}/follower${i}.json" &>/dev/null &
		FOLLOWER_PID[$i]=$!
	done
}

function startFollower() {
	if [ -z "${FOLLOWER_PID[$1]}" ]
	then
		./broker -conf="${CONFIG_PATH}/follower${1}.json" &>/dev/null &
		FOLLOWER_PID[$1]=$!
		echo "started follower $1 at ${FOLLOWER_PID[$1]}"
		N=$((N+1))
	fi
}

function clearLogs {
	cd $BIN_PATH  
        rm ../tmp/* &>/dev/null
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

function killFollower() {
	if [ ! -z "${FOLLOWER_PID[$1]}" ]
	then
		kill ${FOLLOWER_PID[$1]}
		echo "killed follower $1 at ${FOLLOWER_PID[$1]}"
		FOLLOWER_PID[$1]=
		N=$((N-1))
	fi
}

# killFollowers kills the followers
function killFollowers {
	if [ $N -eq 0 ]; then
		return
	fi
	for i in `jot ${N} 1`
	do
		killFollower $i
	done
}

function killAll {
	killRegister
	killLeader
	killFollowers
}

function killOrStart() {
        i=$((RANDOM % 2))
        if [ $i -eq 0 ]
        then
                killFollower $1
        else
                startFollower $1
        fi
}

function passFail() {
	if [ $1 -eq 0 ]
        then
		PASS_COUNT=$((PASS_COUNT+1))
                echo "PASS"     
        else
                echo "FAIL"
        fi
}

function checkLogs {
        cd $BIN_PATH
        cd ../tmp
        TMP_PATH=$(pwd)
        for i in `jot ${N} 1`
        do
                shopt -s nullglob
                for f in $TMP_PATH/*
                do
                        fname=$(basename "$f")
                        diff $f ../tmp-follower${i}/$fname
                        if [ $? -ne 0 ] ; then
                                cd $BIN_PATH
                                return 1
                        fi
                done
        done
        cd $BIN_PATH
        return 0
}

function checkFollowerLogs {
	cd $BIN_PATH
	cd ../tmp-follower1
	FOLLOWER1_PATH=$(pwd)
	for i in `jot ${N} 1`
	do
		shopt -s nullglob
		for f in $FOLLOWER1_PATH/*
		do
			fname=$(basename "$F")
			diff $f ../tmp-follower${i}/$fname
			if [ $? -ne 0 ] ; then
				cd $BIN_PATH
				return 1
			fi
		done
	done
	cd $BIN_PATH
	return 0
}

function testSimpleTransition {
	echo "Starting testSimpleTransition"
	TESTS_TOTAL=$((TESTS_TOTAL+1))
	N=3
	startRegister
	startLeader
	startFollowers
	sleep 3
	killLeader
	./stupidproducer &>/dev/null &
	sleep 3
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
        clearLogs
}

function testRandomTransition {
	echo "Starting testRandomTransition..."
	TESTS_TOTAL=$((TESTS_TOTAL+1))
	NSTART=3
	N=3
	M=20
	startRegister
	startLeader
	startFollowers
	sleep 3
	killLeader
	sleep 2
	for i in `jot ${M} 1`
        do
                ./stupidproducer -id="Producer${i}" &>/dev/null &
		randNum=$(((RANDOM % $NSTART)+1))
                if [ $N -gt 2 ] 
		then
			killOrStart $randNum
		else
			startFollower $randNum
		fi
                sleep 1
        done

	for i in `jot ${NSTART} 1`
	do
		startFollower $i
	done
	sleep 10
	checkFollowerLogs
	passFail $?
	killRegister
	killFollowers
	clearLogs
}

#testSimpleTransition
#testTransitionAdd
testRandomTransition

echo "Passed ${PASS_COUNT}/${TESTS_TOTAL} Tests"
