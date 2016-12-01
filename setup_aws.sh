#!/bin/sh
# this is intended to be run once to sanity check the output, then piped to sh
IPS_ZONE1="54.183.96.145 54.67.110.56"
IPS_ZONE2="52.15.170.53 52.15.124.72"

cargo build

for ip in $IPS_ZONE1; do
    KEY=demo3.pem
    echo scp -i "$KEY" target/debug/kingr4_weinsa_distributedsystems_project2 "ubuntu@${ip}:project2"
    echo scp -i "$KEY" demo_nodes.txt "ubuntu@${ip}:nodes.txt"
done

for ip in $IPS_ZONE2; do
    KEY=demo1.pem
    echo scp -i "$KEY" target/debug/kingr4_weinsa_distributedsystems_project2 "ubuntu@${ip}:project2"
    echo scp -i "$KEY" demo_nodes.txt "ubuntu@${ip}:nodes.txt"
done


echo ssh -i demo3.pem ubuntu@ec2-54-183-96-145.us-west-1.compute.amazonaws.com '"export RUST_LOG=\"kingr4_weinsa_distributedsystems_project2=debug\"; screen -S project2 -d -m; sleep 0.1; screen -S project2 -p 0 -X stuff \"./project2 server 1\\n\""'
echo ssh -i demo3.pem ubuntu@ec2-54-67-110-56.us-west-1.compute.amazonaws.com '"export RUST_LOG=\"kingr4_weinsa_distributedsystems_project2=debug\"; screen -S project2 -d -m; sleep 0.1; screen -S project2 -p 0 -X stuff \"./project2 server 2\\n\""'
echo ssh -i demo1.pem ubuntu@ec2-52-15-170-53.us-east-2.compute.amazonaws.com '"export RUST_LOG=\"kingr4_weinsa_distributedsystems_project2=debug\"; screen -S project2 -d -m; sleep 0.1; screen -S project2 -p 0 -X stuff \"./project2 server 3\\n\""'
echo ssh -i demo1.pem ubuntu@ec2-52-15-124-72.us-east-2.compute.amazonaws.com '"export RUST_LOG=\"kingr4_weinsa_distributedsystems_project2=debug\"; screen -S project2 -d -m; sleep 0.1; screen -S project2 -p 0 -X stuff \"./project2 server 4\\n\""'
