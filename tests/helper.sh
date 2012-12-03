# startLeader starts the leader in the background
function startLeader {
  mkdir -p $TMP_PATH
  ./broker -conf="${CONFIG_PATH}/leader.json" &>/dev/null &
  LEADER_PID=$!
  sleep 5
}

# startRegister starts the register in the background
function startRegister {
  ./register -conf="${CONFIG_PATH}/reg.json" &>~/Desktop/reg.txt &
  REG_PID=$!
  sleep 5
}

# startFollowers starts the followers in the background
function startFollowers {
  if [ $NSTART -eq 0 ]
  then
    return
  fi
  for i in `jot ${NSTART} 1`
  do
    mkdir -p "${TMP_PATH}-follower-${i}"
    ./broker -conf="${CONFIG_PATH}/follower${i}.json" &
    FOLLOWER_PID[$i]=$!
    echo "Started follower $i at ${FOLLOWER_PID[$i]}"
  done
}

function startFollower() {
  if [ -z "${FOLLOWER_PID[$1]}" ]
  then
    ./broker -conf="${CONFIG_PATH}/follower${1}.json" &
    FOLLOWER_PID[$1]=$!
    echo "Started follower $1 at ${FOLLOWER_PID[$1]}"
    N=$((N+1))
  fi
}

function clearLogs {
  rm -r ${TMP_PATH} 2> /dev/null
  rm -r ${TMP_PATH}-follower-* 2> /dev/null
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
    echo "Killed follower $1 at ${FOLLOWER_PID[$1]}"
    FOLLOWER_PID[$1]=
    N=$((N-1))
  fi
}

# killFollowers kills the followers
function killFollowers {
  if [ $NSTART -eq 0 ]; then
    return
  fi
  for i in `jot ${NSTART} 1`
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
  cd ${TMP_PATH}
  for i in `jot ${NSTART} 1`
  do
    shopt -s nullglob
    for f in $TMP_PATH/*
    do
      fname=$(basename "$f")
      diff $f "${TMP_PATH}-follower-${i}/${fname}"
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
  cd $TMP_PATH
  cd "../tmp-follower-1"
  FOLLOWER1_PATH=$(pwd)
  for i in `jot ${NSTART} 1`
  do
    shopt -s nullglob
    for f in $FOLLOWER1_PATH/*
    do
      fname=$(basename "$F")
      diff $f "${TMP_PATH}-follower-${i}/${fname}"
      if [ $? -ne 0 ] ; then
        cd $BIN_PATH
        return 1
      fi
    done
  done
  cd $BIN_PATH
  return 0
}
