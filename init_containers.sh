#!/bin/bash -e

echo "Starting docker containers"

cd dw
sudo docker-compose up -d
sleep 15
cd ..

echo "Done!"
