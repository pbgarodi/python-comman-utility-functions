# -*- coding: utf-8 -*-
__author__ = " Pravin Garodi"

# python dependencies
import uuid
import time
import os
import json
import shutil
import pymysql
import pandas as pd 

# Google cloud dependencies
from google.cloud import storage

class CommonUtility:

    @staticmethod
    def get_connection(self,host,username,password,db_name):
    """this function will return database connection 
    @param:
        host = ip of database server
        username = database username 
        password = database password 
        db_name = database name where you want to perform operation
    @return:
        return connection object
    """
    
    global connection
    try:
        connection = pymysql.connect(host,username,password,db_name)
    except Exception as error:
        print(error)
    return connection

    @staticmethod
    def get_data_from_db(sql_query,connection):
        """
        this function will read data from database and return dataframe.

        @param:
            sql_query = sql statment. 
            connection = database connection object.
        @returns:
            dataframe
        """
        df = pd.read_sql(sql_query, connection)
        return df

    @staticmethod
    def txt_to_csv(self, file_path, csv_file):
        """
        txt_to_csv is used to convert txt to csv
        @param:
        file_path = path of file
        @returns- None
        """
        if file_path.endswith(('.txt')):
            in_txt = csv.reader(open(file_path, 'rb'), delimiter=';')
            out_csv = csv.writer(open(csv_file, 'wb'))
            out_csv.writerows(in_txt)
            return True
        return False

    @staticmethod
    def create_zip(output_filename,dir_name):
        shutil.make_archive(output_filename, 'zip', dir_name)
        return output_filename+'.zip'

    @staticmethod
    def download(bucket_name, file_key, local_file_path):
        """
        download function is used for downloading file from GCS and store that file in local machine
        s
        @param:
            bucket_name = bucket name
            file_key = file key of GCS object
            unique_folder = unique folder key of 32 character

        @returns- if download is succeed function will return True & machine location where file is stored
                else it returns FALSE & failure Message

        @raises exception that could be raised if download is unsuccessful
        """
        message = None
        try:
            message = 'BLANK'
            client = storage.Client()

            # Handling exception when Bucket does not exist

            source_bucket = client.get_bucket(bucket_name)
            # Download file from GCP
            with open(local_file_path, 'wb') as file_obj:
                blob = source_bucket.blob(file_key)
                blob.download_to_file(file_obj)
            message = local_file_path
        except Exception as e:
            print(e)
        return message
    
    @staticmethod
    def generate_uuid():
        """ generate_uuid function is used to generate uuid of 32 character

        @returns:
            This will return 32 character uuid
        """
        return str(uuid.uuid4())

    @staticmethod
    def read_json_files(json_file):
        """
        read_json_files function is used to read the json

        @param:
            json_file = json file

        @returns:
            This will return json object
        """

        with open(json_file, 'r') as file:
            json_file_object = json.load(file)
        return json_file_object

    @staticmethod
    def concat_string(*args):
        """
        concat_string function is used to concatenate the string 

        @param:
            *args = takes variable no. of arguments 

        @returns:
            This will return newly created concatenated string 
        """
        return "".join(map(str, args))

    @staticmethod
    def create_folder(folder_key):
        """
        create_folder function is used to take the folder key
        @param:
            folder_key = path to create folder with folder name e.g.:/tmp/sample_output/
        """
        # create a output folder if not exist
        if not os.path.exists(folder_key):
            os.makedirs(folder_key)

    @staticmethod
    def remove_file(file):
        """
        remove_file function is used to remove the file at specified file key

        @param:
            file = file key

        @returns:
            This will return removed_status as true if file removed successfully else it will return false
        """
        removed_status = False
        if(os.path.isfile(file)):
            os.remove(file)
            removed_status = True

        return removed_status

    @staticmethod
    def remove_directory(request_folder):
        """
        Remove the directory and its content
        @params:
            request_folder = Path to the folder to which we have to delete

        @returns- True
        """
        flag_status = False
        if(os.path.isdir(request_folder)):
            shutil.rmtree(request_folder)
            flag_status = True
        else:
            flag_status = False

        return flag_status

    @staticmethod
    def dumps_json_in_files(json_file, json_data):
        """
        dumps_json_in_files function is used to dumps the json in file

        @param:
            json_file = json file
            json_data = json data dictionary
        """
        file_status = False
        with open(json_file, 'w') as file:
            json.dump(json_data, file)
            file_status = True
        return file_status

    @staticmethod
    def timstamp_to_sec(timestamp):
        """
        timstamp_to_sec function is used to convert timestamp format into seconds

        @params:
            timestamp =  time in format 00:00:00.000

        @returns:
            This will return the timestamp format into (integer) seconds 
        """
        timestamp_list = timestamp.split(':')
        return int(timestamp_list[0]) * 3600 + int(timestamp_list[1]) * 60 + int(timestamp_list[2])


    @staticmethod
    def second_to_timestamp(time_in_second): 
        """
        second_to_timestamp function is used to convert seconds into timestamp format

        @params:
            time_in_second =  time in second

        @returns:
            This will return the timestamp format in hh:mm:ss
        """
        return time.strftime('%H:%M:%S', time.gmtime(time_in_second))