# The Stevens Data Warehouse

The Stevens submission for the Intelligent Data Management course project. A web-based interface for exploring the [Global Terrorism Database](https://www.start.umd.edu/gtd/), utilizing three different database systems for our backend.

## Run project

Simple setup process, downloads all dependencies and sets up everything so that it works with the UI. If you have docker-compose installed already this step can be skipped.

When running the init_db.sh file, you may be asked for your sudo password, this is necessary to start the docker containers.

```rst
$ cd stevensDW/
$ sudo apt install docker-compose
$ pip install -r requirements.txt
$ ./init_db.sh
```

After doing this you can start the UI using the command below, the UI can then be accessed at localhost:8000 in the web browser on your machine.

```rst
$ python app.py
```


## User Manual
After running the setup process, the program can be accessed at [localhost:8000](localhost:8000) in your browser. Where you will be presented with a map, two pie-charts and a query area.

To be able to use the UI you first need to select which backend you would like to be using, MySQL, MongoDB or Neo4J. After doing so, the heatmap, pie-charts and query area will be populated and usable.

![Image of the UI](/data/image.png)

The map is clickable, and shows most countries in our data. When you click a country on the map that has associated events, the pie-charts and the query area will update. The color of the countries represents how many of the total events in the database happened here. Closer to purple means fewer events, and closer to yellow means more events. Ex. Iraq is colored yellow, as it has the most events in the database.

The pie-charts to the right of the map show the number of terrorist attacks per year, and the number of people killed in terrorist attacks per year in the selected country.

The query area allows you to browse the different events in the database. Here you have 6 drop-down menus, allowing you to specify specific settings for your query. These are:

- **Country:** Select which country you want to see events for, leaving this open shows events for all countries. When selecting a country the pie-charts will also be updated. This area also allows you to browse historic countries, not shown on the map, like East-Germany.
- **Start and End year:** these options allow you to set which time period you want to see events from. If setting only Start-year you will only get events from that given year.
- **Attack type:** This option allows you to select only events of specific attacks, e.g. Shootings, or Bombings.
- **Target type:** This option allows you to select only events directed at specific target types, e.g. Government.
- **Success:** This boolean parameter will select only the events which were deemed successfull or not by the perpetrators.


You also here have the options to decide which columns you want to see in the table, meaning if you are not interested in e.g. which city the event happened in, you can deselect it. Giving you full control of the information presented to you.

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
