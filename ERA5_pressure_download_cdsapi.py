#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
# Author:     Andrew Kovachik
# Last Edit:  2018-04-19 17:06:06
# Contact:    andrew.kovachik2@Canada.ca
import os
import sys
import datetime
import argparse
from configparser import ConfigParser
from pandas import date_range
sys.path.append('/home/mas001/computer_programs/python/tropOMI/')
#import tropOMI_toolkit as trop_tool
import cdsapi
from cdsapi import Client
import netCDF4

'''
Download global ERA5 uv wind product from ecmwf.
WARNING: The ERA5 api key will need to be renewed in early 2019.
'''
# def checkfile(filename):
#     try:
#         netCDF4.Dataset(filename)
#     except:
#         print('file is zipped')
#         os.system('mv %s %s.zip'%(filename, filename))
#         os.system('unzip %s'%filename)
#         #mov file to filename
#         os.system('mv ')
def download(date, edir, res,levels, times):

    server = Client()

    print("{dir}/single/ECMWF_ERA5_p_{d:%Y%m}.nc".format(dir=edir, d=date))
    dataset= 'reanalysis-era5-single-levels'
    request = {
    "product_type": ["reanalysis"],
    "variable": [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "2m_dewpoint_temperature",
        "2m_temperature",
        "surface_pressure",
        "total_cloud_cover",
        "boundary_layer_height"
    ],
    "time": [
        "00:00", "01:00", "02:00",
        "03:00", "04:00", "05:00",
        "06:00", "07:00", "08:00",
        "09:00", "10:00", "11:00",
        "12:00", "13:00", "14:00",
        "15:00", "16:00", "17:00",
        "18:00", "19:00", "20:00",
        "21:00", "22:00", "23:00"
    ],
    "data_format": "netcdf",
    "download_format": "unarchived",
        'year': date.strftime("%Y"),
        'month': date.strftime("%m"),
        'day': date.strftime("%d")
        #'grid': [res, res]
        }

    client = cdsapi.Client()
    client.retrieve(dataset, request, "{dir}/single/{d:%Y}/ECMWF_ERA5_p_{d:%Y%m%d}.nc".format(dir=edir, d=date))
       # "{dir}/single/{d:%Y}/ECMWF_ERA5_p_{d:%Y%m%d}.nc".format(dir=edir, d=date))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Download global ERA5 uv wind product from cdsapi. Please "
                    "see cdsapi documentation for more in-depth explanation of "
                    "inputs. This download will not begin automatically, but "
                    "you will be placed into the queue at "
                    "http://apps.ecmwf.int/webapi-activity/ for each day."
    )
    parser.add_argument(
        '-d', '--daterange', required=True, nargs='*',
        help="Date, must have format YYYY Can also "
             "search over range of dates, "
             "simply by providing a second, later date."
    )
    parser.add_argument(
        '-r', '--resolution', default=0.3,
        help="Resolution to download data at. Data will be stored on a regular "
             "longitude/latitude grid of this resolution. Will default to 0.3"
    )
    parser.add_argument(
        '-l', '--levels', required=False,
        help="Height levels to download data at."
             "Should be provided as level with spaces"
    )
    parser.add_argument(
        '-t', '--times', nargs='*', required=False,
        help="UTC times to download data at. Should be formated as HH:MM with spaces"
    )
    parser.add_argument(
        '--directory', required=False,
        help='Directory where the files will be stored. Files will be '
             'sorted by year below this. Will default to the msh/model folder'
    )
    args = parser.parse_args()



    directory = '/home/deg001/deg001/msh/model/ecmwf/ERA5/nc' \
        if not args.directory else args.directory

    levels = ['1000', '950', '900', '850', '800', '750', '700', '650',
              '600', '550', '500', '450', '400', '350' ,'300', '250',
              '200', '150', '100'] if not args.levels else args.levels

    times = ['00:00', '01:00', '02:00', '03:00', '04:00', '05:00', '06:00', '07:00',
             '08:00', '09:00', '10:00 ', '11:00', '12:00', '13:00', '14:00', '15:00',
             '16:00', '17:00', '18:00', '19:00', '20:00', '21:00', '22:00', '23:00'] if not args.times else args.times



    dates = date_range(args.daterange[0], args.daterange[1])
    for day in dates:
        dirs = "{dir}/single/{d:%Y}/".format(dir=directory, d=day)
        if not os.path.exists(dirs):
            os.system('mkdir %s'%dirs)
            print('mkdir %s'%dirs)

        download(day, directory, args.resolution, levels, times)
        #checkfile("{dir}/single/{d:%Y}/ECMWF_ERA5_p_{d:%Y%m%d}.nc".format(dir=directory, d=day) )


