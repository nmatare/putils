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
""" Efficient creation of dynamic SQL statements in python  """

class AdvancedSQLQueries(object):
  """
  Dynamically construct SQL statements for common data-engineering tasks

  WARNING: For implementation reasons, this class does not take advantage of
    SQLs' parameterized (@) functionality. If you are using this class in a 
    production environment, be wary of SQL injection when constructing 
    user input: https://en.wikipedia.org/wiki/SQL_injection
  """
  def __init__(self):
    super().__init__()

  def pivot(self, columns:dict, index:str, value:str, fill="NULL") -> list:
    """
    Pivot a column; otherwise known as: "reshaping long to wide" or
    "dcasting a table"

    Args: 
      index: The index to pivot the table upon. The index should match
        the value(s) of the passed columns dictionary

      columns:
        A dictionary containing the column names(keys) and value(values) to
        generate the pivot queries

      fill:
        (Optional) A value specifying a non-observed pivot

      condense:
        (Optional) If one is conscious of the number of characters in the query,
        set condense to True to squeeze the function into a user defined 
        function that the user will need to define at the base query:

        CREATE TEMP FUNCTION 
        p(i FLOAT64, c FLOAT64, v FLOAT64) AS (
          IF(i = c, v, NULL) -- fill needs to manually entered here
        );     
    """
    pivot = []
    for k, v in columns.items():
      pivot.append(f'MAX(IF({index}={v},{value},{fill})) AS {k}')
    return pivot

  def ffillnull(self, columns:dict, index:str) -> list:
    """
    Forward fill NULL values using the specified method

    Args:
      columns:
        A dictionary containing the column names(keys) to generate the queries

      index:
        The column name of the sorting index to use
    """
    fill = []
    for k, v in columns.items():
      statement = f"""LAST_VALUE({k} IGNORE NULLS) OVER (
        ORDER BY {index} ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS {k}"""
      fill.append(' '.join(statement.split()))
    return fill

  def first_valid_index(self, columns:dict, include_zero:bool=False) -> list:
    """
    Return index for first NULL value

    Note: It's often more efficient to use arrays as opposed to these long 
    winded queries

    Args:
      columns:
        A dictionary containing the column names(keys) to generate the queries

      include_zero:
        (Optional) Should zero be included as a valid index
    """

    valid = []
    for k, v in columns.items():
      if include_zero:
        statement = f" AND {k}!=0 "
      else:
        statement = f" "
      valid.append(f"WHEN {k} IS NOT NULL{statement}THEN {v}")
    return valid

  def exclude_epochs(self, epoch_range, start:str="start", end:str="end"
  ) -> list:
    """
    Dynamically exclude periods of time

    Args:
      epoch_range (pd.DataFrame):
        A pandas.DataFrame specifying the start and end time of each epoch per
        row; e.g. |1| 2018-08-01 | 2018-09-01 |

      start:
        (Optional) The column name specifying the start time of each epoch 

      end:
        (Optional) The column name specifying the end time of each epoch 

    """
    exclude = []
    for k, v in epoch_range.items():
      exclude.append(
        f"AND time NOT BETWEEN '{v[start]}' AND '{v[end]}'")
    return exclude

  def shift(self, columns:dict, periods:int, index:str) -> list:
    """
    Shift the specified columns by a desired number of periods
  
    Args:
      columns:
        A dictionary containing the column names(keys) to generate the queries
      
      periods:
        Number of periods to move, can be positive, negative, or zero

      index:
        The column name of the sorting index to use
    """

    shift = []
    for k, v in columns.items():
      if periods >= 0:
        statement = f"LAG({k},{periods})"
      else:
        statement = f"LEAD({k},{abs(periods)})"
      shift.append(f"{statement} OVER(ORDER BY {index}) AS {k}_T{periods:02d}")
    return shift
