# putils

A hybrid R/Python package containing public utility 
functions/modules for data analysis

* Version 0.0.9
* Development Status :: 2 - Pre-Alpha

## Features:

  * 'create_project', 'start_project' 
  	(scaffolding to quickly start an R/Python project)

  * 'fast_bq_query_with_gcs' (quickly download large datasets from 
      Google BigQuery to client) 

  * 'as.mmap.xts ' (memory mapped method for xts objects; requires 'mmap')


## Installation

R Installation

```R
library(devtools)
devtools::install_github("nmatare/putils", subdir="/R")
```

Python installation

```sh
    pip3 install git+https://github.com/nmatare/putils.git#egg=measurements
```

## Usage:

### Fast Big Query 

```R

output <- fast_bq_query_with_gcs(
   query="SELECT * FROM dataset.table WHERE TIME > 2018 ORDER BY TIME",
   project_id="user_project_id",
   bucket="user_cloud_bucket",
   dataset="user_dataset",
   table="user_table",
   servive="location_to_user_json_service_file",
   path=tempdir(),
   legacy_sql=TRUE,
   download=TRUE,
   export_as="csv.gz"
)
str(output) # data.table created from csv.gz files

```

### Quickly Deployable Project 

```R


```


Author(s)
----
* Nathan Matare 

## License

This project is licensed under the Apache License Version 2.0 - see 
[LICENSE.md](https://github.com/nmatare/putils/blob/master/README.md) 
file for details

