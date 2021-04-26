# Pyspark Starter template
A starter TEMPLATE for pyspark applications with Deltalakes. Includes [Quinn](https://github.com/MrPowers/quinn) & [Chispa](https://github.com/MrPowers/chispa) libraries for smooth programming and testing.

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
TBD

# Contributing
Open to new improvements/changes 🚀 Just feel free to fork this repository and open a PR back with any changes!
