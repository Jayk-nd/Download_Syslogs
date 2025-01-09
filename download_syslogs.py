#!/usr/bin/env python3
#jay
import glob  
import fnmatch  
from datetime import datetime, timedelta
import boto3
import traceback
import sys
import argparse
import os
import gzip
import shutil
import pandas as pd
import os
import shutil
import subprocess
import argparse
import pandas as pd
# import xlsxwriter
import time
from datetime import datetime
import sys
import shutil
from datetime import timedelta
#from OAC import spark_session_class
from OAC.device_issue_classes.issue_common_methods import *
from OAC import DB

prod_db = DB.NewDB('prod')

def get_date_list(startDate):
    dates = []
    try:
        dates.append(startDate)
        start_date= datetime.strptime(startDate, '%Y-%m-%d')
        endDate =  start_date + timedelta(days=3)
        incDate = datetime.strptime(startDate, '%Y-%m-%d')
        while incDate < endDate:
            incDate = (incDate +  timedelta(days=1)).strftime('%Y-%m-%d')
            dates.append(incDate)
            incDate = datetime.strptime(incDate, '%Y-%m-%d')
        return dates
    except:
        # printing stack trace
        traceback.print_exc()
        sys.exit(1)


def append_to_excel(df, file_name, sheet_name='Sheet1'):
    try:
        # Check if the file exists
        try:
            # Try to load existing workbook
            df_existing = pd.read_excel(file_name)
        except FileNotFoundError:
            # If file not found, create an empty DataFrame
            df_existing = pd.DataFrame()

        # Append new data
        df_combined = df_existing.append(df, ignore_index=True)

        # Save the combined data to Excel
        df_combined.to_excel(file_name, index=False, sheet_name=sheet_name)
        
        print(f"Data appended to '{file_name}' successfully.")
    
    except Exception as e:
        print(f"Error appending data to '{file_name}': {str(e)}")

# Function to query result_location based on device_id
def query_result_location(df, device_id):
    try:
        result = df.loc[df['device_id'] == device_id, 'result_location'].iloc[0]
        return result
    except IndexError:
        print(f"No result found for device_id '{device_id}'.")
        return None
    except Exception as e:
        print(f"Error querying result_location: {str(e)}")
        return None
def query_execution_status(df, device_id):
    try:
        result = df.loc[df['device_id'] == device_id, 'action_status'].iloc[0]
        return result
    except IndexError:
        print(f"No result found for device_id '{device_id}'.")
        return None
    except Exception as e:
        print(f"Error querying action_status: {str(e)}")
        return None



def get_command_executed_timestamp(deviceId, KA_command, KA_command2, startDate=None):
    '''
    Function to get command executed time 
    return:
    timestamp when command executed
    '''
    print("inside get_command_executed_timestamp : ",deviceId)
    
    if not startDate:
        command_to_get_executed = """
        SELECT a.device_id, b.title, a.created, a.updated, a.result_location, a.payload, 
               CASE  
                   WHEN a.action_state = 0 THEN 'AWAITING_CONSUMPTION' 
                   WHEN a.action_state = 1 THEN 'CONSUMED'  
                   WHEN a.action_state = 2 THEN 'EXECUTED' 
                   WHEN a.action_state = 3 THEN 'CANCELED' 
                   WHEN a.action_state = 5 THEN 'FORCE_KILLED_BY_MAX_RETRY'    
                   ELSE 'unknown'  
               END AS action_status    
        FROM nddeviceaction a    
        LEFT JOIN ndcommandlineactionpayload b ON a.payload = b.payload 
        WHERE a.device_id IN ('{}') AND b.title in ('{}', '{}')
        ORDER BY a.updated DESC LIMIT 1 
        """.format(deviceId, KA_command, KA_command2)
    else:
        command_to_get_executed = """
        SELECT a.device_id, b.title, a.created, a.updated, a.result_location, a.payload, 
               CASE  
                   WHEN a.action_state = 0 THEN 'AWAITING_CONSUMPTION' 
                   WHEN a.action_state = 1 THEN 'CONSUMED'  
                   WHEN a.action_state = 2 THEN 'EXECUTED' 
                   WHEN a.action_state = 3 THEN 'CANCELED' 
                   WHEN a.action_state = 5 THEN 'FORCE_KILLED_BY_MAX_RETRY'    
                   ELSE 'unknown'  
               END AS action_status    
        FROM nddeviceaction a    
        LEFT JOIN ndcommandlineactionpayload b ON a.payload = b.payload 
        WHERE a.device_id IN ('{}') AND b.title in ('{}', '{}') 
              AND a.updated >= '{}' 
              AND a.updated < ('{}'::date + '1 day'::interval) 
        ORDER BY a.updated DESC LIMIT 1 
        """.format(deviceId, KA_command, KA_command2, startDate, startDate)
    
    print(command_to_get_executed)
    command_to_get_executed_devices = prod_db.runCmd(command_to_get_executed, cursor_factory=True)
    command_executed_time = pd.DataFrame(data=command_to_get_executed_devices)
    
    # Check if DataFrame is empty
    if command_executed_time.empty:
        print(f"\033[91m No command executed timestamp found. Exiting the function. \n -> deviceId = {deviceId} \033[0m")
        return None
        sys.exit(0)
    
    print("------------------------------------------")
    print(f"\033[91m{command_executed_time.head()}\033[0m")
    print("------------------------------------------")
    
    # file_name = 'devices_data.xlsx'
    # sheet_name = 'Sheet1'
    
    # Append DataFrame to Excel
    # append_to_excel(command_executed_time, file_name, sheet_name)

    # result_location = query_result_location(command_executed_time, deviceId)
    # action_status = query_execution_status(command_executed_time, command_executed_time)
    print("***************************")

    timestamp = command_executed_time.at[0, 'updated']
    print(command_executed_time.columns)
    print(timestamp)

    return timestamp

def create_s3_bucket_object(bucket):
    try:
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(bucket)
        return bucket
    except:
        # printing stack trace
        traceback.print_exc()
        sys.exit(1)


def find_files(directory, filename):
    """
    Recursively searches for all occurrences of a file in a directory and its subdirectories.

    Args:
        directory (str): The path to the directory to search in.
        filename (str): The name of the file to search for.

    Returns:
        list: A list of all file paths that match the filename in the directory and its subdirectories.
    """
    file_list = []
    # Loop through all directories and files in the directory and its subdirectories
    for dirpath, dirnames, filenames in os.walk(directory):
        for name in filenames:
            if fnmatch.fnmatch(name, filename):
                file_list.append(os.path.join(dirpath, name))
            # Check if the file name matches the desired file name
    # Return the list of file paths
    return file_list

def extract_logs(base_path):
    for filename in os.listdir(base_path):
        try:
            if '.zip' in filename:
                unzip_cmd = '7z x ' + base_path + filename + ' -o./' + base_path + filename[:-4] + '/ -y'
                os.system(unzip_cmd)
            elif '.7z' in filename:
                unzip_cmd = '7z x ' + base_path + filename + ' -o./' + base_path + filename[:-3] + '/ -y'
                os.system(unzip_cmd)
        except:
            # printing stack trace
            traceback.print_exc()
            print("extract log failed",filename)
            continue

def remove_log_extension(filename):
    """
    Removes '.log' from the end of a filename.

    Args:
        filename (str): The filename to modify.

    Returns:
        str: The modified filename.
    """
    if filename.endswith('.log'):
        return filename[:-4]
    else:
        return filename

def convert_time_to_epoch(timestamp):
    try:
        # Try to parse the timestamp with milliseconds
        dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        try:
            # If parsing with milliseconds fails, try without milliseconds
            dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        except ValueError as e:
            # If both formats fail, handle the error (e.g., print an error message)
            print(f"Error parsing timestamp: {timestamp}. Error: {e}")
            return None

    # Convert the datetime object to epoch time (seconds since 1970-01-01 00:00:00)
    epoch_time = int(time.mktime(dt.timetuple()))

    return epoch_time


def download_sys_logs(bucket, deviceId, startDate, command_executed_timestamp):
    dates = get_date_list(startDate)
    for index in ["0", "1", "2", "3", "4"]:
        print(index, "log server index")
        for date_str in dates:
            prefix = "logs_" + index + "/" + deviceId + '/' + date_str
            try:
                files_list = bucket.objects.filter(Prefix=prefix)
                if len(list(files_list)) > 0:
                    for log_file in files_list:
                        dest_path = 'SYS_LOGS/' + log_file.key[7:]
                        if not os.path.exists(os.path.dirname(dest_path)):
                            os.makedirs(os.path.dirname(dest_path))
                        print(log_file.key)
                        print(log_file.key.split("/")[-1].split(".")[0].split("_")[0][:-3],"=====================") 
                        file_timestamp = int(log_file.key.split("/")[-1].split(".")[0].split("_")[0][:-3])
                        if file_timestamp > command_executed_timestamp:
                            print("After timestamp")
                            bucket.download_file(log_file.key, dest_path)
                            extract_path = 'SYS_LOGS/' + deviceId + '/' + date_str + '/'
                            extract_logs(extract_path)
                            search_path = 'SYS_LOGS/' + deviceId + '/' + date_str + '/' + log_file.key.split("/")[-1].split(".")[0] + "/"
                            filelist = find_files(search_path, "syslog*")
                            print(log_file.key, dest_path, extract_path, filelist, search_path)
                            if not filelist:
                                remove_zip_files = 'rm -f ' + dest_path
                                os.system(remove_zip_files)
                                remove_search_path = 'rm -rf ' + search_path
                                os.system(remove_search_path)
                            else:
                                print(filelist)
                                print(log_file.key)
                                return (filelist, log_file.key)
                else:
                    print("logs does not exist for the date continue", date_str)
                    dir_path = 'SYS_LOGS/' + deviceId
                    os.makedirs(dir_path, exist_ok=True)
                    continue
            except:
                # printing stack trace
                traceback.print_exc()
                sys.exit(1)
    return None

def clean_directory(path):  
    # Ensure the provided path is a directory  
    if not os.path.isdir(path):  
        raise ValueError(f"The path {path} is not a valid directory.")  
      
    # Iterate through all items in the directory  
    for item in os.listdir(path):  
        item_path = os.path.join(path, item)  
          
        # Check if the item's name starts with 'syslog'  
        if not item.startswith('syslog'):  
            try:  
                # Remove directories  
                if os.path.isdir(item_path):  
                    shutil.rmtree(item_path)  
                    print(f"Removed directory: {item_path}")  
                # Remove files  
                elif os.path.isfile(item_path):  
                    os.remove(item_path)  
                    print(f"Removed file: {item_path}")  
            except Exception as e:  
                print(f"Error removing {item_path}: {e}")  

def main():
    parser = argparse.ArgumentParser(description='Optional app description')
    parser.add_argument('-d','--deviceId', type=str,help='deviceId',required=True)
    parser.add_argument('-sd','--StartDate', type=str,help='Start Date')
    parser.add_argument('-ka','--ka_command', type=str,help='KA Command')
    args = parser.parse_args()
    deviceId = args.deviceId
    deviceId = deviceId.strip()
    StartDate = args.StartDate
    bucket = "idms-production"
    ka_command=args.ka_command
    print(deviceId)
    if deviceId.startswith("6") or deviceId.startswith("2"):
        KA_command="krait_seperated_syslog_v1 (2023-07-25:05:27:49 GMT)"
        KA_command2="Krait_Syslogs_3 (2021-01-07:15:22:21 GMT)" 
    elif deviceId.startswith("10") or deviceId.startswith("3"):
        KA_command="Seperated_SysLogs (2018-09-26:15:14:49 GMT)"
    elif not ka_command:
        KA_command = ka_command
    else:
        print("unkonwn ka command")
        exit()
    KA_command2="Krait_Syslogs_3 (2021-01-07:15:22:21 GMT)"
    # StartDate = '2024-11-12'
    StartDate_time = get_command_executed_timestamp(deviceId,KA_command,KA_command2,StartDate)

    if StartDate_time is None:
        print("-------------- Timestamp is not present --------------------------")
        return

    StartDate = str(StartDate_time).split(" ")[0]
    Start_date_time = str(StartDate_time)    
    command_executed_timestamp = convert_time_to_epoch(Start_date_time)
  
    bucket = create_s3_bucket_object(bucket)
    result = download_sys_logs(bucket,deviceId,StartDate,command_executed_timestamp)
    
    if result is None:
        print("No logs found.")
        return

    (filelist, s3Link) = result
    for file_path in filelist:
        print(file_path)
        dst_dir = 'SYS_LOGS/' +deviceId+ "/"
        new_name = remove_log_extension(dst_dir+"/"+file_path.split("/")[-1])    
        subprocess.run(['cp', "-f" ,file_path, new_name])
        if new_name.endswith(".tar.gz"):
            extract_cmd = ['tar', '-xzvf', new_name, '-C', dst_dir]
            subprocess.run(extract_cmd,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            gz_files = glob.glob(dst_dir + "/var/log/*.gz")  
        if new_name.endswith(".gz"):
            gz_files = glob.glob(dst_dir + "/*.gz")

    dst_dir = 'SYS_LOGS/' +deviceId+ "/"
    clean_directory(dst_dir)
    
    prod_db.closeConnection()

if __name__ == "__main__":
    main()