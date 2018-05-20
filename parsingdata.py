#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Create a column based dataset by parsing zipped json files obtained from FDA
of adverse events
"""

import os
import json
import time
import zipfile
import glob
import csv

basedir = "/Users/balaji/Documents/AZdataanalysis"
downloadfile =  os.path.join(basedir, "data/download.json")
datadir = os.path.join(basedir, "data/drugadverseevents")


# Read the json file in the zipped format directly
def readjson(file):
    with zipfile.ZipFile(file) as zf:
        for filename in zf.namelist():
            with zf.open(filename) as zfile:
                databytes = zfile.read()
                data = json.loads(databytes.decode('utf8'), object_hook = obj)
            zfile.close()
        zf.close()
    return (data)


# Function used to ensure there is only one json file in each zipped file
def numjson(file):
    with zipfile.ZipFile(file) as zf:
        return (len(zf.namelist()))



class obj:
    def __init__(self,data):
        self.__dict__ = data


# Definition of the datapoint (report) with only columns taken for analysis
# This can be changed to add appropriate columns
class Datapoint:    
    def __init__(self, record):
        self.id = record.safetyreportid
        self.version = getattr(record,'safetyreportversion', '')
        self.country = getattr(record.primarysource, 'reportercountry','')
        self.serious = getattr(record,'serious','')
        self.patientonsetage = getattr(record.patient,'patientonsetage','')
        self.patientonsetageunit = getattr(record.patient,'patientonsetageunit','')
        self.patientsex = getattr(record.patient,'patientsex','') 
        self.seriousnesshospitalization = getattr(record,
                                                  'seriousnesshospitalization','')
        self.submitter = getattr(record.primarysource, 'qualification','')

        # Attributes that are lists and need to be combined 
        self.drugs = ""
        self.drugindication = ""
        self.reactions = ""
        
        
        # Get all the drugs associated with the report
        drugs = set()
        drugindication = set()
        for drug in record.patient.drug:
            try:
                drugs.add(drug.openfda.generic_name[0])
            except:
                pass
            
            try:
                drugindication.add(drug.drugindication)
            except:
                pass

            
        # Get all the reactions associated with the ADE report
        reactions = set()
        for entry in record.patient.reaction:
            try:
                reactions.add(entry.reactionmeddrapt)
            except:
                pass
            
        # Remove the trailing semicolon in the string
        self.drugs = ";".join(drugs)
        self.drugindication = ";".join(drugindication)
        self.reactions = ";".join(reactions)
        
    # Function to return the appropriate formatted string for exploration                    
    def __str__(self):
        return ('{0.id},{0.version},{0.serious},{0.seriousnesshospitalization},{0.country},'
                '{0.patientonsetage},{0.patientonsetageunit},{0.patientsex},'
                '{0.reactions},'
                '{0.drugs},{0.drugindication}\n'.format(self))
        
    # Function to return attributes as a dictionary
    #def returnattr(self):
    #    return (self.id, self.version, self.serious, self.seriousnesshospitalization,
    #            self.country, self.patientonsetage, self.patientonsetageunit,
    #            self.patientsex, self.reactions, self.drugs, self.drugindication)
        
    
            
                        
# Reading the input Json file and creating a dataset for exploration        
t1 = time.clock()  

outfilename = os.path.join(basedir, "data/datasetfile.csv")

# Write appropriate header for the output file
header = ["id", "version", "serious", "seriousnesshospitalization", "country",
          "submitter","patientonsetage","patientonsetageunit","patientsex", "reactions",
          "drugs","drugindication"]

# Parse through all directories and all files within each directory
with open(outfilename,"w") as outfile:
    outfile_csv = csv.DictWriter(outfile, header)
    outfile_csv.writeheader()
    
    filenum = 0
    dnum = 0
    for d in filter(os.path.isdir, glob.glob(datadir+"/*")):
        for file in glob.glob(d+"/*.zip"):
            filenum += 1
            print("Processing filenum: ", filenum)
            data = readjson(file)        
            for record in data.results:
                # Projecting the json record to a flattened version
                dp = Datapoint(record)
                
                # Write the output to a file
                outfile_csv.writerow(dp.__dict__)
        dnum += 1
        #if dnum > 0:
        #    break

outfile.close()            


t2 = time.clock()
print("Time taken (in sec): ", t2-t1)
        
        
        
        
        


