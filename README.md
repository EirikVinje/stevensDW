# The Stevens DataWareHouse

## dependencies
- sudo apt install docker-compose
- pip install -r requirements.txt
- terrorist dataset in csv

## container commands 
- sudo docker-compose up -d (runs the containers)
- sudo docker-compose down -v (stops and removes the containers)


## Run project
```rst
$ cd stevensDW/
$ sudo apt install docker-compose
$ pip install -r requirements.txt
$ cd dw/
$ sudo docker-compose up -d
```

## To run MySQL databases odb and dw after docker-compose
```rst
$ sudo docker exec -it dw_mysql-odb-server_1 bash
$ mysql -p
$ CREATE USER 'dbuser'@'%' IDENTIFIED BY 'secret';
$ GRANT ALL PRIVILEGES ON odb.* to 'dbuser'@'%';
$ exit
$ exit
$ sudo docker exec -it dw_mysql-dw-server_1 bash
$ mysql -p
$ CREATE USER 'dbuser'@'%' IDENTIFIED BY 'secret';
$ GRANT ALL PRIVILEGES ON dw.* to 'dbuser'@'%';
$ exit
$ exit
$ python prepare_sql.py
$ python populate_sql.py
```

## In browser, access databases

- Neo4j : localhost:7474
- MySQL-odb : localhost:15000
- MySQL-dw: localhost:25000
- Mongodb : 
