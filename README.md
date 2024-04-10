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
$ python init_neo4j.py
```

## In browser, access databases

- Neo4j : localhost:7474
- MySql : localhost:
- Mongodb : 