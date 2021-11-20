# Project: Data Warehouse

## Table of Contents
- [Project: Data Warehouse](#project-data-warehouse)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Project structure](#project-structure)
    - [Folder: notebooks](#folder-notebooks)
    - [Files](#files)

## Introduction

The Sparkify startup wants to move its processes and data onto the cloud. Regard that this project was to build an ETL pipeline loading data from its given files stored in an AWS bucket to a database hosted in a Redshift cluster. 

## Installation

Download de project source.

Create a virtual environment (using python >= 3.5)

```console
$ python -m venv .venv
$ source .venv/bin/activate
```

Install project dependencies using Pip package manager.

```console
$ pip install -r requirements.txt
```

## Usage

To set up the AWS infrastructure resources needed for executing the required ETL was used Python boto3 framework. In light of this, to initialize the infrastructure, you can set the AWS variables (key and secret) in the 'dwh.cfg' file and run the command line as follow:

```console
$ python aws.py up
```

To remove the created resources, you can run the command line as follow:

```console
$ python aws.py down
```

The above command will not delete s3 songs file paths resources created at the initialization. For that, you can run the command line as follow: 

```console
$ python aws.py bucket-delete [bucket-name]
```

To create or restore tables used by ETL process, from project folder in the terminal, you can run the command line as follow:

```console
$ python create_tables.py
```

To execute the ETL script from project folder:

```console
$ python etl.py
```

## Project structure

### Folder: notebooks

* test.ipynb - Notebook used to test SQL queries.
* exploring_files.ipynb - Notebook used to explore s3 files.

### Files

* aws.py - cli tool for creating and removing AWS resources.
* cluster.py - a python module that helps create and remove AWS resources.
* create_tables.py - drop and create tables.
* etl.py - reads and processes files from s3 files and loads them into tables.
* dwc.cfg - project configurations.