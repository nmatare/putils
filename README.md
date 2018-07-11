# putils

A hybrid R/Python package containing public utility functions/modules for data analysis

* Version 0.0.9
* Development Status :: 2 - Pre-Alpha

## Features:

  * `init_project`, `start_project` - scaffolding to quickly start and deploy an R/ Python project(analysis )

  * `fast_bq_query_with_gcs` - quickly download large datasets from Google BigQuery

  * `TimeDimension` - methods to efficiently create panel pandas/dask.DataFrames and spatio-temporal xarray.DataArrays/dask.Arrays from cross-sectional data, __at scale__

  * `as.mmap.xts` - memory mapped method for R xts objects


## Installation

R Installation

```R
library(devtools)
devtools::install_github("nmatare/putils", subdir="/R")
```

Python installation

* Prerequisites:
- Install [spark-avro](https://github.com/databricks/spark-avro) from Databricks

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
# Init Setup
if(!library(putils, logical.return=TRUE)) 
  devtools::install_github("nmatare/putils", subdir="/R", reload=TRUE)

# If project doesn't exist, a new project will be created
project_name <- "my_project" 
putils::init_project(
  project=project_name, 
  python=3,
  r_config_options=paste(
    "# R Convenience Options",
    "options(width=100)",
    "options(digits.secs=3)"
  )
)

# For an already created project, running the script loads the configuration files and custom project settings
project_name <- "my_project"
start_project(project_name)

```

### TimeDimension 

```python
from putils.timed import TimeDimension
from pandas import pd
from numpy import np
import dask

timeMethods = TimeDimension() 
df = pd.DataFrame(np.random.randint(0,100, size=(100, 4)), columns=list('ABCD'))
data = dask.dataframe.from_pandas(df, npartitions=3)


# Lag Features as Panel Data
data = timeMethods.lag_features(data, lag=20)
panel_data = data.compute()
panel_data.head()

# Convert pandas.DataFrame to xarray.DataArray
xarray = timeMethods.reshape_panel_to_xarray(panel_data, lag=20)
xarray

# Convert dask.DataFrame to numpy.Array
data = timeMethods.lag_features(data, lag=20) 
data = reshape_to_daskarray(data, lag=20) # see help()
array_data = data.compute()
array_data

```

Author(s)
----
* Nathan Matare 

## License

This project is licensed under the Apache License Version 2.0 - see 
[LICENSE.md](https://github.com/nmatare/putils/blob/master/README.md) 
file for details

