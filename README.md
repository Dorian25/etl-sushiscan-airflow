# etl-sushiscan-airflow

I created this program to `extract` data from the website [sushiscan.su](https://sushiscan.su/) (a manga online reading site in french), `load` them into a MongoDB database and then use their `Data API`.

`The MongoDB Atlas Data API lets you read and write data in Atlas with standard HTTPS requests. To use the Data API, all you need is an HTTPS client and a valid API key.`

The final goal is to provide daily updated data to one of my projects [Get-mangas](https://github.com/Dorian25/get-mangas).

Access to the dashboard MongoDB Charts : [link](https://charts.mongodb.com/charts-getmanga-rhtkb/public/dashboards/632df18e-f274-4d69-899d-21740a3f593f)

## Table of content
- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Run the DAG](#run-the-dag)
- [Contributing](#contributing)
- [Credits](#credits)
- [Disclaimer](#disclaimer)
- [License](#license)

## Installation

**Apache Airflow** : 2.3.4

**OS** : Xubuntu 22.04 LTS


### 1. Install dependencies
```
sudo apt-get install libmysqlclient-dev
sudo apt-get install libssl-dev
sudo apt-get install libkrb5-dev
```
### 2. Install Apache Airflow
```
#
pip3 install apache-airflow

#
pip3 install typing_extensions

#
pip3 install apache-airflow-providers-mongo
```

### 3. Initialize the database
Run this command :
```
# it will create a airflow.cfg dans le dossier ~/airflow
airflow init db
```
Then, run the following to create a user :
```
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```


## Run the DAG
### 1. Configure a connection
###

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## Credits
- Mangas are collected on [Sushi-Scan](https://sushiscan.su/)
- `requirements.txt` was generated with the library [pipreqs](https://github.com/bndr/pipreqs)

## Disclaimer
:sparkling_heart: Please support the publishers and authors by purchasing their works :sparkling_heart: 

## License
[MIT](https://choosealicense.com/licenses/mit/)
