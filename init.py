# Databricks notebook source
from pyspark.sql.functions import current_timestamp, rand
from pyspark.sql.functions import col, count
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
import datetime
from pyspark.sql.types import StringType
