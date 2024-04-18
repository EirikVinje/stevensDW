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
$ ./dw/init_db.sh
```


## In browser, access databases

- Neo4j : localhost:7474
- MySQL-odb : localhost:15000
- MySQL-dw: localhost:25000
- MongoDB has no web interface available


## Features available:
- (get all events for all countries)
- get_num_events_all_countries(): get number of events in all countries
- get_events_by_country(): get events in specific country
- get_events_with_criteria(): TABLE WITH COUNTRY, START YEAR, END YEAR, ATTACKTYPE, TARGETTYPE, SUCCESS
