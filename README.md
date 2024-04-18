# The Stevens DataWareHouse

This is the repo for our DW project.

## Run project

Simple setup process, downloads all dependencies and sets up everything so that it works with the UI. If you have docker-compose installed already this step can be skipped.

```rst
$ cd stevensDW/
$ sudo apt install docker-compose
$ pip install -r requirements.txt
$ ./init_db.sh
```

Access UI at: localhost:8000




## User Manual
After running the setup process, the program can be accessed at [localhost:8000](localhost:8000) in your browser. Where you will be presented with a map, 2 pie charts and a query area.

To start the UI you first need to select which backend you would like to be using, MySQL, MongoDB or Neo4J. After doing so all the features of the UI will be working.

The map is clickable, and shows most countries in our data. When you click a country on the map that has associated events, the pie-charts and the query area will update. 

The pie-charts to the right of the map show the number of terrorist attacks per year, and the number of people killed in terrorist attacks per year in the selected country.

The query area allows you to browse the different events in the database. Here you have 6 drop-down menus, allowing you to specify specific settings for your query. These are:

- **Country:** Select which country you want to see events for, leaving this open shows events for all countries. When selecting a country the pie-charts will also be updated. This area also allows you to browse historic countries, not shown on the map, like East-Germany.
- **Start and End year:** these options allow you to set which time period you want to see events from. If setting only Start-year you will only get events from that given year.
- **Attack type:** This option allows you to select only events of specific attacks, e.g. Shootings, or Bombings.
- **Target type:** This option allows you to select only events directed at specific target types, e.g. Government.
- **Success:** This boolean parameter will select only the events which were deemed successfull or not by the perpetrators.


## Remove docker containers for databases

If you wish to remove the databases and their containers, you can run the following command from the stevensDW directory. **NOTE:** You will need to run the entire setup process again if you wish to use the databases again.

```rst
$ ./remove_db.sh
```


## In browser, access databases

- Neo4j : localhost:7474
- MySQL-odb : localhost:15000
- MySQL-dw: localhost:25000
- MongoDB has no web interface available
