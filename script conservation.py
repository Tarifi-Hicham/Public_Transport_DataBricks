# Databricks notebook source
# MAGIC %run "./script import csv"

# COMMAND ----------

def get_file_duration(path):
    modification_time_ms = path.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to datetime
    duration = (datetime.now() - modification_time).total_seconds() / 60
    return duration

# COMMAND ----------

def archived_raw_files(raw_paths):
    for path in raw_paths:
        file_duration = get_file_duration(path)
        # check if the duration 
        if file_duration >= 15:
            # get the raw directory
            source_directory = path.path
            # get the archived directory
            destination_directory = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public_transport_data/archived/{path.name}"
            dbutils.fs.mv(source_directory, destination_directory,recurse = True)

# COMMAND ----------

def delete_archived_files(archived_paths):
    for path in archived_paths:
        file_duration = get_file_duration(path)
        # check if the duration 
        if file_duration >= 30:
            # get the raw directory
            source_directory = path.path
            # get the archived directory
            destination_directory = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/public_transport_data/archived/{path.name}"
            dbutils.fs.rm(destination_directory,recurse = True)

# COMMAND ----------

from datetime import datetime

storage_account_name = "tarifihicham1cs"
storage_account_access_key = "OCGL4AOQKWaFu6lezWKGDCVXDe7534tiifLMFUgdrPm6YJ3Vff3CMX5EGbxwIXGgBkdqnO6xomBP+ASti5On2w=="
container_name = "tarifihichamcontainer"

# get files path
files_paths = get_file_path(storage_account_name,storage_account_access_key,container_name)

archived_raw_files(files_paths[0])
delete_archived_files(files_paths[2])

