# Pyspark Starter template
Example implementation for Spark Kampus use cases using PySpark and Kafka.
# Run the application

## Dependencies
Dependecies are automatically managed by **Poetry** and there is NO need to use external dockers for running spark.

To install dependencies run
```bash
poetry install
```
in same folder where your `.toml` file is located. 
Poetry will take care of:
- Installing the required Python interpreter 
- Installing all the libraries and modules 
- Creating the virtual environment for you

## Running on local

Start the process with the command:
```
python main/main.py  
```

## Run Tests
You can run all test scenarios using:
```
python -m pytest
```

# Repository Setup

- `main.py`: Main entrypoint for creating + configuring the Spark session and launching the process.
- `orchestrator`: Contains each of the processing steps of the pipeline. Responsible of managing the load, process and store of the results.
- `normalizer`: Validates and normalizes raw input data. Creates extra columns and renames/formats to align with the normalized events.
- `processor`: Runs the queries over the input datframes to produce the expected outputs.


# Contributing
Open to new improvements/changes 🚀 Just feel free to fork this repository and open a PR back with any changes!
