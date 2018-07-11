#!/usr/bin/python3
# -*- coding: utf-8 -*-
#
#  Copyright 2016-2018 Nathan Matare 
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

""" Fast Big Query query with Google Cloud Storage """

from google.cloud.bigquery import Client as BQclient
from google.cloud.storage import Client as GCSclient
from google.cloud.bigquery import QueryJobConfig
from google.cloud.bigquery import ExtractJobConfig
from google.cloud import storage
from tempfile import mkstemp as create_temp_directory

class FastBigQueryRetrieval(object):
    """
	Deprecated in favor of R version 

    Args:
        service_file (str):
            The path/filename of a JSON service account keyfile

        dataset (str):
            The path/filename of a JSON service account keyfile

        export_as (str):
            The path/filename of a JSON service account keyfile

        verbose (bool):
            The path/filename of a JSON service account keyfile

        temp_path (str):
            The path/filename of a JSON service account keyfile

    Raises:
        AsyncAuthGoogleCloudError:
            Raised if cannot retrieve authentication token

    """
	def __init__(self, service_file, dataset, export_as = "csv.gz", 
				 verbose=True, temp_path=create_temp_directory()[1]):
		self.bq_client = BQclient.from_service_account_json(service_file)
		self.gcs_client = GCSclient.from_service_account_json(service_file)

		self._use_legacy_sql = use_legacy_sql
		self._verbose = verbose
		self._temp_path = temp_path
		self._base_name = self._temp_path.replace("/tmp", "")
		self.temp_table = self.bq_client.dataset(dataset).table(
			self._base_name + "_temp_table")

		assert export_as in ["csv.gz", "csv", "avro"]
		self._export_as = export_as

	def _config_query(self, use_legacy_sql):
		job_config = QueryJobConfig()
		job_config.destination = self.temp_table
		job_config.use_legacy_sql = use_legacy_sql
		job_config.allow_large_results = True
		return job_config

	def send_query(self, query, use_legacy_sql):
		query_job = self.bq_client.query(query=query, 
			job_config=self._config_query(use_legacy_sql=use_legacy_sql))
		query_job.result() # blocking

	def _config_export(self):
		export_config = ExtractJobConfig()
		export_config.compression = 
			"GZIP" if self._export_as == "csv.gz" else "NONE" 
		export_config.destination_format = 
			"AVRO" if self._export_as == "avro" else "CSV"
		export_config.print_header = False
		return export_config

	def send_to_gcs(self, bucket):
		extract_job = self.bq_client.extract_table(
			source=self.temp_table, 
			job_config=self._config_export(),
			destination_uris=(
				f'gs://{bucket}/{self._base_name}*.{self._export_as}'))
		extract_job.result() # blocking

	def delete_temp_table(self):
		self.bq_client.delete_table(self.temp_table)

	def download_from_gcs(self, bucket):
		bucket_ref = self.gcs_client.get_bucket(bucket)
		blob_refs = bucket_ref.list_blobs(prefix=self.base_name)

		for blob_ref in blob_refs:
			blob_ref = bucket_ref.get_blob(blob_ref.name)
			temp_file = f'{self._temp_path}/{blob_ref.name}'
			with open(temp_file, "wb") as file:
				blob_ref.download_to_file(file)
			bucket_ref.delete_blob(blob_ref.name)

	def read_csv(self, file_path, **kwargs):
		# read into pandas

	def read_avro(self, file_path, blocksize=1048576):
        """
       	Read the downloaded query into avro

        Args:
            file_path (str):
                A message from the GDAX websocket API

            blocksize (int):
				Size of blocks. Note that this size must be larger than the 
				files' internal block size, otheriwse it will result in 
				empty partitions. Default is 1MB

        :rtype: dask.delayed
        :returns: a dask delayed object

        """		
		from dask_avro import read_avro
		from dask.bag import from_delayed

		delayed_avro = read_avro(file_path, blocksize=blocksize)
		return from_delayed(delayed_avro)






