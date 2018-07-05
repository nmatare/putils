#!/usr/bin/Rscript
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# "Author: Nathan Matare <nathan.matare@chicagobooth.edu>"
#
#' @title 
#' 'fast_bq_query_with_gcs'
#'
#' @description 
#' Fast retrieval of user data from Google Big Query. The queried data are 
#' transferred to the user's specified Google Cloud Storage bucket
#' before the entire result is returned as a singular 'data.table' object.
#' 
#' @details 
#' For large amounts of data, this is the preferred method to retrieve queries. 
#' A user must specify a SQL query to Google Big Query. Big Query will run the 
#' query and return the results to a temporarily table created in the user's
#' specified dataset. The temporary table will then be transferred to Google 
#' Cloud Storage. The temporary files are then read directly into a data.table via
#' data.table::fread. All temporary files are then removed (both on the client
#' (locally) and on the server (cloud))
#' 
#' This function uses the python API to interact with Google Big Query and 
#' Google Cloud Storage. The library data.table::fread is used to read 'csv' or 
#' 'csv.gz' files. Therefore, you must first install the 'data.table', 
#' 'reticulate', and 'unixtools' R libraries, along with the below 
#' python google.cloud client libraries:
#' 
#' pip install --upgrade google-cloud-bigquery
#' 
#' pip install --upgrade google-cloud-storage
#' 
#' @param query         A SQL query to be executed. 
#' 
#' @param legacy_sql    (optional) Boolean control for legacy SQL syntax 
#'                      defaults to True
#' 
#' @param export_as     (optional) The exported file format. Possible values 
#'                      include 'csv', 'csv.gz', or 'avro.' Defaults to 'csv.gz'
#' 
#' @param project_id    Project name/id for the project which the client acts 
#'                      on behalf of
#' 
#' @param bucket        A bucket found in the user's GCS specifying where the 
#'                      queried results shall be downloaded into
#' 
#' @param dataset       A character vector of length one identifying 
#'                      the desired dataset
#'
#' @param table         A character vector of length one identifying 
#'                      the desired table in aforementioned dataset
#'
#' @param service_file  The path to a private key file (this file was given to 
#'                      you when you created the service account)
#'                      This file must be a JSON object including a private key 
#'                      and other credentials information (downloaded from 
#'                      the Google APIs console)
#' 
#' @param verbose       (optional) Boolean control for progress display
#' 
#' @param download      (optional) Boolean control whether compressed files 
#'                      should be automatically downloaded, or remain in GCS
#'                      Note: You must manually delete the temporary files 
#'                      from your GCS bucket if you don't desire to have 
#'                      this function explicitly download the files
#' 
#' @param path          (optional) A character vector of length one specifying 
#'                      the desired temporary location to download queries 
#'                      results into. Defaults to base::tempdir()
#' 
#' @param ...           (optional) Any pass-through arguments (**kwargs) to 
#'                      data.table::fread()
#' 
#' @return
#' 
#' a data.table of class 'data.table' containing:
#' \describe{  
#' \item{data.table:}{A data.table of class 'data.table' containing 
#' the queried data}
#' }
#'
#' @references  
#' https://cloud.google.com/bigquery/docs/cached-results
#' 
#' https://cloud.google.com/bigquery/docs/reference/rest/v2/jobs
#' 
#' https://cloud.google.com/bigquery/docs/writing-results
#' 
#' https://google-cloud-python.readthedocs.io/en/latest/bigquery/generated/
#' google.cloud.bigquery.job.QueryJobConfig.html?highlight=QueryJobConfig
#' 
#' https://google-cloud-python.readthedocs.io/en/latest/bigquery
#' /usage.html#load-table-data-from-google-cloud-storage
#'
#' @keywords    fast Big Query Google Cloud Storage
#'
#' @author      Nathan Matare <email: nmatare@chicagobooth.com>
#'
#' @examples
#' \dontrun{
#' output <- fast_bq_query_with_gcs(
#'    query="SELECT * FROM dataset.table WHERE TIME > 2018 ORDER BY TIME",
#'    project_id="user_project_id",
#'    bucket="user_cloud_bucket",
#'    dataset="user_dataset",
#'    table="user_table",
#'    servive="location_to_user_json_service_file",
#'    path=tempdir(),
#'    legacy_sql=TRUE,
#'    download=TRUE,
#'    export_as="csv.gz"
#' )
#' str(output) # data.table created from csv.gz files
#' 
#' }
#' @export
#' 
fast_bq_query_with_gcs <- function(query, project_id, bucket, dataset, table, 
                                   service_file, verbose=TRUE, download=TRUE, 
                                   path=tempdir(), legacy_sql=TRUE, 
                                   export_as='csv.gz', ...          
){

  export_as <- match.arg(export_as, c("csv", "csv.gz", "avro"))
  stopifnot(all(sapply(c("google.cloud.storage", "google.cloud.bigquery"),
    reticulate::py_module_available)))

  bigquery = reticulate::import("google.cloud.bigquery", convert=FALSE)
  storage = reticulate::import("google.cloud.storage", convert=FALSE)
  py = reticulate::import_builtins()

  client = bigquery$Client$from_service_account_json(service_file)
  base_name <- gsub("/","", tempfile("temp_bq_", tmpdir=""))

  .query_bigquery <- function(client, query, bigquery, base_name, 
                              dataset, verbose, legacy_sql)
  {
    job_config = bigquery$QueryJobConfig()
    table_ref = client$dataset(dataset)$table(paste0(base_name, "_temp_table"))   
    job_config$destination = table_ref
    job_config$use_legacy_sql = legacy_sql
    job_config$allow_large_results = TRUE
    query_job = client$query(query=query, job_config=job_config)
    
    if(verbose)
      cat("Querying Google BigQuery \n")

    query_job$result()
    TRUE
  }

  stopifnot(.query_bigquery(
    client, query, bigquery, base_name, dataset, verbose, legacy_sql))
  
  .send_to_gcs <- function(client, bigquery, base_name, dataset, 
                           verbose)
  {
    export_config = bigquery$ExtractJobConfig()
    export_config$compression = 
      if(export_as == "csv.gz") "GZIP" else "NONE"
    export_config$destination_format = 
      if(export_as == "avro") "AVRO" else "CSV"
    export_config$print_header = FALSE

    table_ref = client$dataset(dataset)$table(
      paste0(base_name, "_temp_table")) 

    extract_job = client$extract_table(
      source=table_ref,
      destination_uris=paste0(
        'gs://', bucket, "/", base_name, "*.", export_as),
      job_config=export_config
    )
    if(verbose)
      cat("Transferring data to Google Cloud Storage \n")

    extract_job$result()
    TRUE
  }

  stopifnot(.send_to_gcs(client, bigquery, base_name, dataset, verbose))

  .delete_temp_table <- function(client, dataset, base_name){
    table_ref = client$dataset(dataset)$table(paste0(base_name, "_temp_table"))
    client$delete_table(table_ref)
    TRUE
  }

  stopifnot(.delete_temp_table(client, dataset, base_name))

  if(download){

    client = storage$Client$from_service_account_json(
      json_credentials_path=service_file)
    bucket_ref = client$get_bucket(bucket)

    blob_refs = py$list(bucket_ref$list_blobs(prefix=base_name)) # temp blobs
    blobs <- sapply(blob_refs, function(blob) bucket_ref$get_blob(blob$name))

    if(verbose)
      cat(paste("Retrieving", export_as, "files from GCS \n"))

    .download_blob_to_temp_file <- function(blob){
        blob_ref <- bucket_ref$get_blob(blob$name)
        temp_file <- file.path(path, blob$name)
        with(py$open(temp_file, "wb") %as% file, {
          blob_ref$download_to_file(file)
        })
        TRUE
    }

    .remove_temp_file <- function(temp_file){
        temp_file <- file.path(path, temp_file)
        suppressWarnings(file.remove(temp_file))
        TRUE
    }

    base::dir.create(path, showWarnings=FALSE)
    results <- lapply(blobs, .download_blob_to_temp_file)

    if(!all(unlist(results)))
      stop(paste("Errors downloading", export_as ,"files"))

    temp_names <- list.files(path=path, 
      pattern=paste0(base_name, ".*.", export_as))
    bucket_ref$delete_blobs(blobs)

    if(Sys.info()["sysname"] != "Windows")
      unixtools::set.tempdir(path)

    if(export_as != "avro"){
      if(verbose)
        cat(paste("Reading", export_as ,"into memory as data.table \n"))

      dt <- data.table::fread(
        input=paste0(
          if(export_as == "csv.gz") "zcat " else "", path, "/" , paste( 
          temp_names, collapse=" ")), # identical to gunzip -c
        showProgress=verbose,
        ...=...
      )

    } else {

      if(verbose)
        cat(paste("Concatenating avro files \n"))

      # @TODO fancy call the Java class directly with rJava and .jcall
      run_jar <- system.file("java", "avro-tools-1.8.2.jar", 
        package="putils", mustWork=TRUE)
      command <-  paste("java -jar", run_jar, "concat", 
                    paste(file.path(path, temp_names), collapse=" "), 
                      file.path(path, paste0(base_name, ".avro")))

      if(system(command, ignore.stdout=TRUE, ignore.stderr=TRUE) != 0)
        stop(paste("Errors concatenating .avro files"))

      cat(paste0("The queried results are available in ", 
        file.path(path, base_name), "; use Spark to read the data \n"))
      dt <- NULL
    }

    stopifnot(all(unlist(lapply(temp_names, .remove_temp_file))))
    return(dt)

  } else 
    invisible(cat(paste0(
      "Finished; results are available in bucket: ", bucket, "\n")))
}

