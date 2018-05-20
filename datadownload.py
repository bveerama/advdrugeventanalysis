# Program to automatically download the adverse drug event data from openFDA

# We use the download link from the download.json file from the link below
# https://api.fda.gov/download.json

import os
import json
import time
import subprocess

basedir = "/Users/balaji/Documents/AZdataanalysis"
downloadfile =  os.path.join(basedir, "data/download.json")
datadir = os.path.join(basedir, "data/drugadverseevents")

class obj:
    def __init__(self,data):
        self.__dict__ = data

# Get the JSON file to a python dictionary
with open(downloadfile,"r") as f:
    downloaddic = json.load(f,object_hook = obj)

    
# Estimate the total size of the files to be downloaded
total_size = 0
for entry in downloaddic.results.drug.event.partitions:
    total_size += float(entry.size_mb)
print("Total Size of files to be downloaded: %f MB"%total_size)


# Get the names of directories we expect and create them
# File names clash possible for different years+quarters
directories = set()
for entry in downloaddic.results.drug.event.partitions:
    dirname = entry.file.split("/")[-2] # year and quarter
    directories.add(dirname)

for d in directories:
    path = os.path.join(datadir, d)
    os.mkdir(path)
    

# Iterate through the appropriate dictionary and download files to destination
faileddownloads = []
filesdownloaded = 0
for entry in downloaddic.results.drug.event.partitions:
    link = entry.file
    opdir = os.path.join(datadir, link.split("/")[-2])
    cmd = "wget " + link + " -P " + opdir   
    try:
        status = subprocess.check_output(cmd, shell=True)
        time.sleep(1)
        filesdownloaded += 1
    except subprocess.CalledProcessError as e:
        faileddownloads.append(link)
        print("Download Failed: ", link)
    
    if (filesdownloaded % 25 == 0):
        print('{0} files downloaded'.format(filesdownloaded))
    

       
if (faileddownloads):
    print("Check some downloads failed")