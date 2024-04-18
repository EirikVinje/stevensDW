echo "Removing docker containers for databases..."

cd dw
sudo docker-compose down -v --remove-orphans
sleep 3
cd ..

echo "Done!"