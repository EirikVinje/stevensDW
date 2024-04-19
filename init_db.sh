#!/bin/bash -e

echo "Populating databases mongo, neo4j and sql..."

dbpass="secret"
dbuser="dbuser"

sudo docker exec -it dw_mysql-odb-server_1 bash -c "mysql -P15000 -p${dbpass} -e \"CREATE USER 'dbuser'@'%' IDENTIFIED BY 'secret';\"; exit;"
sudo docker exec -it dw_mysql-odb-server_1 bash -c "mysql -P15000 -p${dbpass} -e \"GRANT ALL PRIVILEGES ON odb.* to 'dbuser'@'%';\"; exit;"

sudo docker exec -it dw_mysql-dw-server_1 bash -c "mysql -P25000 -p${dbpass} -e \"CREATE USER 'dbuser'@'%' IDENTIFIED BY 'secret';\"; exit;"
sudo docker exec -it dw_mysql-dw-server_1 bash -c "mysql -P25000 -p${dbpass} -e \"GRANT ALL PRIVILEGES ON dw.* to 'dbuser'@'%';\"; exit;"

echo "Created MySQL users"


echo ""
echo "Initializing Neo4J"
python dw/init_neo4j.py all
sleep 3

echo "Initializing MongoDB"
python dw/init_mongodb.py 
sleep 3

echo "Initializing MySQL"
python dw/init_sql.py
sleep 3


clear

echo "Done!"