#!/bin/bash -e

echo "Initializing databases mongo, neo4j and sql..."

echo "kuk" | sudo -S docker-compose up -d

sleep 10

python init_neo4j.py all
