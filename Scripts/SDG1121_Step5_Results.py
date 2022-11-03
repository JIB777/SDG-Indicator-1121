#James Gibson
#06/08/2022

#SDG Indicator 11.2.1
#Step 5: Results

import arcpy, sys, os
from collections import defaultdict
from arcpy import env
from arcpy.sa import *
import time
import datetime
import pandas as pd
import numpy as np
import multiprocessing
arcpy.env.overwriteOutput = True

print('Starting  Script...')
outGDB = r'G:\HumanPlanet\SDG1121\Version3\Results\Mapping.gdb'
gadm = r'G:\HumanPlanet\GADM\GADM.gdb\GADM_admin0_iso'

arcpy.env.workspace = r'G:\HumanPlanet\SDG1121\Version3\Countries'
workspaces = arcpy.ListWorkspaces("*", "FileGDB")




#Copy result feature classes to mapping.gdb
for gdb in workspaces:
    try:
        workspace = gdb
        arcpy.env.workspace = workspace
        iso = gdb[-7:-4]
        complete = '%s_complete' % iso
        out_copy = r'G:\HumanPlanet\SDG1121\Version3\Results\Mapping.gdb\%s_copy' % complete
        arcpy.CopyFeatures_management(complete,out_copy)
        print('copied: %s' % complete)
    except:
        print('error: %s' % gdb)
        pass



#merge to create global
workspace = outGDB
arcpy.env.workspace = workspace
merge_list = []
fcs = arcpy.ListFeatureClasses()
for fc in fcs:
    if 'complete_copy' in fc:
        try:
            merge_list.append(fc)
        except:
            print('error: %s' % iso)
            pass
    else:
        pass


length = len(merge_list)
print('Number of Fcs Merged: %s' % length)
out_merge = 'SDG1121_Global_Results_08122022_V3'
arcpy.Merge_management(merge_list,out_merge)
print('merged')

#copy
out_copy = r'G:\HumanPlanet\SDG1121\Version3\Results\Results.gdb\SDG1121_Global_Results_08122022_V3'
arcpy.CopyFeatures_management(out_merge,out_copy)
print('copied')

print('DONE')


    
    

