#!/bin/bash -e

echo "Starting docker containers"

cd dw
sudo docker-compose up -d
sleep 5
cd ..

clear

echo "Done!"
