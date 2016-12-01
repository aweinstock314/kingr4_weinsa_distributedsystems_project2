#!/bin/sh
# this is intended to be run once to sanity check the output, then piped to sh
IPS_ZONE1="54.183.96.145 54.67.110.56"
IPS_ZONE2="52.15.170.53 52.15.124.72"

cargo build --release

for ip in $IPS_ZONE1; do
    KEY=demo1.pem
    echo scp -i "$KEY" target/release/kingr4_weinsa_distributedsystems_project2 "ubuntu@${ip}:project2"
    echo scp -i "$KEY" demo_nodes.txt "ubuntu@${ip}:nodes.txt"
done

for ip in $IPS_ZONE2; do
    KEY=demo3.pem
    echo scp -i "$KEY" target/release/kingr4_weinsa_distributedsystems_project2 "ubuntu@${ip}:project2"
    echo scp -i "$KEY" demo_nodes.txt "ubuntu@${ip}:nodes.txt"
done
