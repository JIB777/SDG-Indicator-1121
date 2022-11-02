#06/07/2022

#SDG Indicator 11.2.1
#Step 4: Analysis

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

#Global Variables
GADMGlobal = r'G:\HumanPlanet\GADM\GADM.gdb\GADMCopy3'
UCDB = r'G:\HumanPlanet\UCDB\Version2\GHS_STAT_UCDB2015MT_GLOBE_R2019A\JG_UCDB.gdb\GHS_STAT_UCDB2015MT_GLOBE_R2019A_V1_2'
public_transport_point = r'G:\HumanPlanet\OSM_data_2022\public_transport_EPSG4326 (1)\public_transport_EPSG4326.gpkg\main.public_transport_EPSG4326_point'

#Start Time
Start_Time = time.time()

def process(iso):
    message = None
    if message is None:
        try:
            gdb = r'G:\HumanPlanet\SDG1121\Version3\Countries\%s.gdb' % iso
            arcpy.env.workspace = gdb
            #Get Data
            out_ucdb = '%s_ucdb' % iso
            Dissolved_output = '%s_dissolve' % iso
            #Get WP
            WP = r'G:\HumanPlanet\WorldPopData\unconstrained\2020\%s\%s_ppp_2020_UNadj.tif' % (iso,iso.lower())
            #Assign UCDBIDs to UCDB clip
            arcpy.AddField_management(out_ucdb,'UCDBID','TEXT')
            counter = 0
            with arcpy.da.UpdateCursor(out_ucdb,['UCDBID']) as cursor:
                for row in cursor:
                    row[0] = counter
                    cursor.updateRow(row)
                    counter = counter + 1
            #Complete
            complete = '%s_complete' % iso
            arcpy.CopyFeatures_management(out_ucdb,complete)
            #Zonal Stats to compute total pop of each ucdb polygon
            arcpy.env.snapRaster = WP
            arcpy.env.cellSize = WP
            outtablename = '%s_UCDB_POP_Table' % iso
            ZonalStatisticsAsTable(complete,"UCDBID",WP,outtablename,"DATA","SUM")
            arcpy.JoinField_management(complete,"UCDBID",outtablename,"UCDBID",["SUM"])
            new_zone = 'Total_POP' 
            arcpy.AlterField_management(complete,"SUM",new_zone,new_zone)
            #Create Zones
            #Intersect
            ucdb_pt_intersect = '%s_ucdb_pt_intersect' % iso
            inFeatures = [Dissolved_output,out_ucdb]
            arcpy.analysis.PairwiseIntersect(inFeatures,ucdb_pt_intersect, "ALL", None, "INPUT")
            #Zonal Stats to compute pop with access
            arcpy.env.snapRaster = WP
            arcpy.env.cellSize = WP
            outtablename2 = '%s_Access_POP_Table' % iso
            ZonalStatisticsAsTable(ucdb_pt_intersect,"UCDBID",WP,outtablename2,"DATA","SUM")
            arcpy.JoinField_management(ucdb_pt_intersect,"UCDBID",outtablename2,"UCDBID",["SUM"])
            new_zone = 'Access_POP'
            arcpy.AlterField_management(ucdb_pt_intersect,"SUM",new_zone,new_zone)
            #Join Field
            pop_with_access_field = 'Access_POP'
            total_pop_field = 'Total_POP'
            arcpy.JoinField_management(complete,"UCDBID",ucdb_pt_intersect,"UCDBID",[pop_with_access_field])
            #Add percent access field
            pct_access_field = 'pct_access' 
            arcpy.AddField_management(complete,pct_access_field,'DOUBLE')
            expression = "(!%s!/!%s!)*100" % (pop_with_access_field,total_pop_field)
            arcpy.CalculateField_management(complete,pct_access_field,expression,"PYTHON")
            message = 'Done: ' + iso
        except Exception as e:
            message = 'Failed: ' + iso + ' ' + str(e)

    return message

def main():
    print('Starting Script...')
    mylist=[]
    with arcpy.da.SearchCursor(UCDB,['CTR_MN_ISO']) as cursor:
        for row in cursor:
            if row[0] in mylist:
                pass
            else:
                mylist.append(row[0])
    length = len(mylist)
    print("Ready to start processing {} countries".format(length))
    pool = multiprocessing.Pool(processes=20, maxtasksperchild=1)
    results = pool.imap_unordered(process,mylist)
    counter = 0
    for result in results:
        print(result)
        counter = counter + 1
        print("{} countries processed out of {}".format(counter,length))
        print('---------------------------------------------------------')
    pool.close()
    pool.join()
    End_Time = time.time()
    Total_Time = End_Time - Start_Time
    print('Total Time: %s' % str(Total_Time))
    print('Script Complete')
    
    


if __name__ == '__main__':
    main()


