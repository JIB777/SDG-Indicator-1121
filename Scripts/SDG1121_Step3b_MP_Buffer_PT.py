#06/07/2022

#SDG 11.2.1
#Step 3: Buffer public transport

#UN Metadata was updated 09/2021
#The access to public transport is considered convenient when a stop is
#accessible within a walking distance along the street network of 500 m from
#a reference point such as a home, school, work place, market, etc. to a low-capacity
#public transport system (e.g. bus, Bus Rapid Transit)
#and/or 1 km to a high-capacity system (e.g. rail, metro, ferry).

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
            #Get PT
            pt_points_filtered = '%s_osm_pt_points_filtered' % iso
            #Split into high and low capacity
            high_capacity_points = '%s_high_capacity' % iso
            arcpy.CopyFeatures_management(pt_points_filtered,high_capacity_points)
            with arcpy.da.UpdateCursor(high_capacity_points,["capacity"]) as cursor:
                for row in cursor:
                    if row[0] == 'High':
                        pass
                    else:
                        cursor.deleteRow()
            low_capacity_points = '%s_low_capacity' % iso
            arcpy.CopyFeatures_management(pt_points_filtered,low_capacity_points)
            with arcpy.da.UpdateCursor(low_capacity_points,["capacity"]) as cursor:
                for row in cursor:
                    if row[0] == 'Low':
                        pass
                    else:
                        cursor.deleteRow()

            #Buffer high capacity points by 1 km and dissolve
            high_cap_buff = '%s_high_capacity_buf' % iso
            arcpy.PairwiseBuffer_analysis(high_capacity_points, high_cap_buff, "1 kilometer", "#","#","GEODESIC")
            
            #Buffer low capacity points by 500 m and dissolve
            low_cap_buff = '%s_low_capacity_buf' % iso
            arcpy.PairwiseBuffer_analysis(low_capacity_points, low_cap_buff, ".5 kilometer", "#","#","GEODESIC")

            #Merge
            out_merge = '%s_buffer_merge' % iso
            arcpy.Merge_management([high_cap_buff,low_cap_buff],out_merge)

            #Dissolve
            out_dissolve = '%s_dissolve' % iso
            arcpy.PairwiseDissolve_analysis(out_merge,out_dissolve)
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


