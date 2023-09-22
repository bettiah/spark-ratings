# IMDB Ratings Processor Using Spark

This repository contains a standalone Spark Application tailored for handling the
IMDB [dataset](https://developer.imdb.com/non-commercial-datasets/).

You can seamlessly import this repository into an IDE, such as IntelliJ. To initiate the application, run
the `MovieRatings` class. This action will activate a local Spark runner, with the results showcased in the console.

Tests can be run from within IDE or from cli using `mvn package`

## Important Points

- Java 11 & Maven 3.9 are required

- Ensure to download the data files from [imdb](https://datasets.imdbws.com/) to a local directory first.

- Currently, the directory path from where the files are processed is preset (LOAD_PREFIX variable). Adjust this
  according to your local configuration.
