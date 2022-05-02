# Data pipelines with Apache AirFlow

## This repository helps data engineers get familiar with data pipelines.

### Usage
Just start a postgres instance in [Postgres official docker page](https://hub.docker.com/_/postgres) or locally run it in your machine.
Next just run `docker-compoe up -d` (where `-d` is to show output trace).

after running docker container *api* service and *airflow* services run simultaneously.

*api* will make some fake data then `dag01` will trace those data and ingest them in postgres database.

**Note**: Table in database in named *test_table*. Be careful to change it with you appropriate name.
Before running `dag01` you should make *database connection* from [Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html).



