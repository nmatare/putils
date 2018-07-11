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
# 
""" Create panel DataFrames and spatio-temporal Arrays  """

from xarray import DataArray
import dask
from dask import delayed
import pandas as pd
import numpy as np

class TimeDimension(object):
    """
    Simple methods to efficiently create panel pandas/dask.DataFrames and 
    spatio-temporal xarray.DataArrays/dask.Arrays from cross-sectional data, 
    __at scale__

    Uses the excellent Dask and Xarray libraries:
        Dask: http://dask.pydata.org/en/latest/docs.html
        Xarray: http://xarray.pydata.org/en/stable/index.html

    """
    def __init__(self):
        """
        """        
        super().__init__()

    def lag_features(self, dataframe, lag):
        """
        Lag each observation in the provided dataframe by previous 'lag' 
        number of observations:

        Given an ordered partition of cross-sectional data, '_create_panel_data'
        will lag each observation(n) by 'lag' number of previous observations, 
        before exporting the expanded result as a MultiIndexed pandas.DataFrame

        Implementation Notes: 
        Given how dask.dataframe.map_overlap works, (during step 4 and 5, 
        the partition is trimmed from the beginning and end of the partition) 
        this method cannot directly return an array (of any form) back to the 
        user. It must first be run (by calling .compute()) before being passed 
        to any downstream functions that would convert the expanded dataframe 
        into an array

        Args:
            dataframe (dask.DataFrame):
                A sorted dask.DataFrame

            lag (int):
                The number of previous observations

        :rtype      operation
        :returns:   A dask.DataFrame containing the expanded and now 
                    MultiIndexed pandas.DataFrame

        """     
        assert lag > 0
        assert isinstance(dataframe, (dask.dataframe.DataFrame, dask.bag.Bag))
        operation = dataframe.map_overlap(
            func=lambda part: self._create_panel_data(part, lag=lag), 
            before=lag, after=0, meta=dict(features=float))

        return operation

    def _create_panel_data(self, partition, lag):
        """
        Args:
            partition (pandas.DataFrame):
                A partition given as a pandas DataFrame

            lag (int):
                The number of previous observations

        :rtype pandas.DataFrame
        :returns: A __MultiIndexed__ pandas.DataFrame
        """
        assert len(partition) > lag
        return pd.concat(
            objs=[partition.slice_shift(lag) for lag in range(0, lag+1)], 
            axis=1, 
            keys=[f't-{l}' for l in range(0, lag+1)], 
            copy=False, 
            sort=True
        )

    def _get_dimensions(self, *args): # pass pandas.DataFrame, lag
        return len(args[0]), args[1]+1, int(len(args[0].columns) / (args[1]+1))

    def reshape_panel_to_xarray(self, dataframe, lag):
        """
        Converts a pandas.DataFrame into an xarray.DataArray. This function
        is helpful after creating a Panel dask.DataFrame, and one is iterating 
        through its partitions

        """
        assert isinstance(dataframe, pd.DataFrame)
        observations, lags, features =  self._get_dimensions(dataframe, lag)
        export = DataArray(
            data=dataframe.values.reshape(observations, lags, features),
            coords=[
                list(dataframe.index),
                list(dataframe.columns.levels[0].values), 
                list(dataframe.columns.levels[1].values)
            ],
            dims=["observations", "lags", "features"])
        return export

    def reshape_to_daskarray(self, dataframe, lag):
        """
        Reshapes a Panel (MultiIndexed) dask.DataFrame into a dask.Array

        Implementation Notes:
        (1) data.values will not work because the partition sizes are unknown
                https://github.com/dask/dask/issues/3090
                https://stackoverflow.com/questions/37444943/dask-array-
                from-dataframe

        (2) reshaping an xarray.DataArray is currently useless because
            dask.concatenate (although the method 'works' on xarray.DataArray) 
            does not preserve meta data; i.e., coords and dimensions

        (3) apparently, the class environment is not exported to workers so the 
            methods but be scoped within the reshape_to_daskarray function

        """
        assert lag > 0

        @delayed
        def _reshape_panel_to_nparray(dataframe, lag):
            """
            Converts a pandas.DataFrame into a numpy.Array. Unlike 
            'reshape_panel_to_xarray', this will loose the inherent 
            MultiIndex metadata (coordinates and dimensions). For this reason, it 
            is used as an intermediate calculation
            """
            observations, lags, features = _get_dimensions(dataframe, lag)
            export = np.array(dataframe.unstack(level=[-1]).values
                ).reshape(observations, lags, features)
            return export

        @delayed(nout=1)
        def _concat_nparray(*args):
            return dask.array.concatenate(args[0], axis=0)

        assert isinstance(dataframe, dask.dataframe.DataFrame)
        partitions = dataframe.to_delayed() # @delayed
        arrays = [_reshape_panel_to_nparray(part, lag) 
                  for part in partitions]

        return _concat_nparray(arrays)

    def write_panel_data(self, dataframe, file_path, **kwargs):
        """
        Simple wrapper method around 'to_hdf': Parallel write 
        """
        assert isinstance(dataframe, dask.dataframe.DataFrame)
        operation = dataframe.to_hdf(
            file_path, "/partition-*", compute=False, **kwargs)

        return operation

    def read_panel_data(self, file_path, **kwargs):
        """
        Simple wrapper method around 'read_hdf': Parallel read 
        """
        operation = dask.dataframe.read_hdf(file_path, "/partition-*", **kwargs)
        return operation



