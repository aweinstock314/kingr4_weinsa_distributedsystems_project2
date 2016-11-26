#!/usr/bin/env sh
SESSIONNAME=kingr4_weinsa_distributedsystems_project2_localdeploy
PIDS=$(seq 1 6)

echo "Compiling the project"
cargo build

echo "Attempting to kill the old deployment (if it exists)"
screen -S $SESSIONNAME -X quit
if test $? -eq 0; then echo "Successful"; fi
sleep 0.1

export RUST_LOG='kingr4_weinsa_distributedsystems_project2=trace'
export RUST_BACKTRACE=1

echo "Creating a new deployment in screen session $SESSIONNAME"
screen -S $SESSIONNAME -d -m
sleep 0.1

for pid in $PIDS; do
    echo "Spawning window for pid $pid"
    screen -S $SESSIONNAME -X screen "$pid"
    screen -S $SESSIONNAME -p $pid -X title "server-$pid"
    screen -S $SESSIONNAME -p $pid -X stuff "./target/debug/kingr4_weinsa_distributedsystems_project2 server $pid\n"
done

sh

for pid in $PIDS; do
    echo "Halting the server with pid $pid"
    screen -S $SESSIONNAME -p $pid -X stuff ""
done
