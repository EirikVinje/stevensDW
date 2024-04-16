#!/bin/bash -e

echo "Sudo password:"

read -s password

echo ""
echo "Initializing databases mongo, neo4j and sql..."

echo $password | sudo -S docker-compose up -d

sleep 10

echo ""
echo "Initializing databases..."

python init_neo4j.py ic
# python init_mongo.py 
# python init_sql.py

echo "Done!"