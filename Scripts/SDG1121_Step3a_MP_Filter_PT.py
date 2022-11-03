#James Gibson
#06/07/2022

#SDG Indicator 11.2.1
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
            out_pt_point = '%s_osm_pt_points' % iso
            pt_points_filtered = '%s_osm_pt_points_filtered' % iso
            arcpy.Delete_management(pt_points_filtered)
            arcpy.CopyFeatures_management(out_pt_point,pt_points_filtered)
            #Filter
            arcpy.AddField_management(pt_points_filtered,'capacity','TEXT')
            pt_keeplist = ['bus_station','bus_stop','platform','platform;stop_position','pole',
                           'station','stop','stop_area','stop_position','stop_position;platform',
                           'stop_position;station','yes']
            railway_keeplist = ['stop','stop_position','platform','station','subway_entrance']
            with arcpy.da.UpdateCursor(pt_points_filtered,["public_transport","railway","amenity","bus","highway","capacity"]) as cursor:
                for row in cursor:
                    #High Capacity criteria
                    if (row[1] in railway_keeplist) or (row[2] == 'ferry_terminal'):
                        row[5] = 'High'
                        cursor.updateRow(row)
                    #Low Capacity criteria
                    elif (row[1] == 'tram_stop') or (row[2] == 'bus_station') or (row[3] == 'yes') or (row[4] == 'bus_stop'):
                        row[5] = 'Low'
                        cursor.updateRow(row)
                    else:
                        pass
            #Default is low
            with arcpy.da.UpdateCursor(pt_points_filtered,["public_transport","capacity"]) as cursor:
                for row in cursor:
                    if (row[0] in pt_keeplist) and (row[1] is not None):
                        pass
                    elif (row[0] in pt_keeplist) and (row[1] is None):
                        row[1] = 'Low'
                        cursor.updateRow(row)
                    else:
                        cursor.deleteRow()
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


