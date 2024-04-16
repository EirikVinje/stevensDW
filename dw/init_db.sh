#!/bin/bash -e

password=$1

echo "Initializing databases mongo, neo4j and sql..."

echo input | sudo -S docker-compose up -d

sleep 10

python init_neo4j.py all
