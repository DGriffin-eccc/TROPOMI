#you will need to create a .netrc file with the credentials for earthdata - save it to your home directory
#inside the file is just one line, save it in your home directory under the name .netrc:

#machine urs.earthdata.nasa.gov login [insert your username] password [insert your password]

#the simply run the script in your termianl using, options for species are no2, hcho, o3:
#python tempo_earthdata_download.py -d 2024.01.01 2024.01.10 -s no2 -v v3
# you need to change teh root_directory root = ... to your directory

from pprint import pprint
from time import asctime, sleep
import earthaccess
import os
# Import Python packages

# Module for manipulating dates and times
import datetime
import pandas as pd
# Module to set filesystem paths appropriate for user's operating system
from pathlib import Path

import os
import argparse
import datetime
import sys
import numpy as np
import netCDF4

def log_message(msg):
    print('{}: {}'.format(asctime(),msg))

# Get product abbrevation used in TROPOMI file name
# "product": parameter variable from widget menu, set in main function


# Limit number of download files for each product
# Since I'm going to make this a cron job that runs
# each night I want to be sure it doesn't runs for
# ever. In one day we may have a maximum of 
# ~15(scans) x 10 (granules) so I will set a limit
# to download no more than 5 days at the time 15x10x5=750
max_down_files = 800
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Download TEMPO files "

    )
    parser.add_argument(
        '-d', '--daterange', required=True, nargs='*',
        help="date, must have format yyyy.mm.dd. can also "
             "search over range of dates, "
             "simply by providing a second, later date."
    )
    parser.add_argument(
        '-s', '--species', required=True,
        help='Name of the chemical species to combine. Should match that in '
             'the TEMPO data file name.'
    )

    parser.add_argument(
        '--dir', default=None,
        help='Directory where the tropOMI files will be stored. This should '
             'point to the format directory in the structure, and all sub-'
             'directories should be arranged in <YYYY>/'
    )
    parser.add_argument(
        '-g', '--granule', required=False,
        help='Specify if you only wish to download a specific granule, options are 1-10.'
    )
    parser.add_argument(
        '-sw', '--swath', required=False,
        help='Specify if you only wish to download a specific swath, options are 0-13.'
    )
    parser.add_argument(
        '-v', '--version', required=True,
        help='Data product version.'
    )

    args = parser.parse_args()
    spec = args.species.upper()
    ver = args.version #if args.version else 'NRTI'  # TODO do much ver better (38)
    dates = pd.date_range(args.daterange[0], args.daterange[1])
    # Define place to hold data records
    root = '/home/mas001/msh/satdata/level2/tempo/'
    print(root)
    # To avoid showing progress bar
    os.environ['TQDM_DISABLE']="1"

    # Authenticate with "Earthdata Login"
    print('authorizing now...')
    auth = earthaccess.login(strategy="netrc")

    # are we authenticated?
    if not auth.authenticated:
        # ask for credentials and persist them in a .netrc file
        auth.login(strategy="interactive", persist=True)

    # Define Collection IDs for each TEMPO data product
    # Following collection IDs will change with a new version
    cid = {}

    # Level-1
    # cid['l1-dark'] = "C2724057022-LARC_CLOUD" # TEMPO dark exposure [TEMPO_DRK_L1_V01]
    # cid['l1-dark-v2'] = "C2842836142-LARC_CLOUD" # TEMPO dark exposure [TEMPO_DRK_L1_V02]
    # cid['l1-dark-v3'] = "C2930729926-LARC_CLOUD" # TEMPO dark exposure [TEMPO_DRK_L1_V03]
    # cid['l1-irrr'] = "C2724056242-LARC_CLOUD"  # TEMPO solar irradiance (reference diffuser) [TEMPO_IRRR_L1_V01]
    # cid['l1-irrr-v2'] = "C2724056242-LARC_CLOUD"  # TEMPO solar irradiance (reference diffuser) [TEMPO_IRRR_L1_V02]
    # cid['l1-irrr-v3'] = "C2930728569-LARC_CLOUD" # TEMPO solar irradiance (reference diffuser) [TEMPO_IRRR_L1_V03]
    # cid['l1-irr'] = "C2724055205-LARC_CLOUD"  # TEMPO solar irradiance [TEMPO_IRR_L1_V01]
    # cid['l1-irr-v2'] = "C2842852230-LARC_CLOUD"  # TEMPO solar irradiance [TEMPO_IRR_L1_V02]
    # cid['l1-irr-v3'] = "C2930757598-LARC_CLOUD"  # TEMPO solar irradiance [TEMPO_IRR_L1_V03]
    # cid['l1-rad'] = "C2724057249-LARC_CLOUD"  # TEMPO geolocated Earth radiances [TEMPO_RAD_L1_V01]
    # cid['l1-rad-v3'] = "C2930759336-LARC_CLOUD"  # TEMPO geolocated Earth radiances [TEMPO_RAD_L1_V03]
    # cid['l1-radt-v3'] = "C2930766795-LARC_CLOUD" # TEMPO geolocated Earth radiances twilight [TEMPO_RADT_L1_V03]

    # Level-2
    # cid['l2-cld'] = "C2724037909-LARC_CLOUD"  # TEMPO cloud pressure and fraction (O2-O2 dimer) [TEMPO_CLDO4_L2_V01]
    # cid['l2-cld-v2'] = "C2842848626-LARC_CLOUD"  # TEMPO cloud pressure and fraction (O2-O2 dimer) [TEMPO_CLDO4_L2_V02]
    # cid['l2-cld-v3'] = "C2930760329-LARC_CLOUD"  # TEMPO cloud pressure and fraction (O2-O2 dimer) [TEMPO_CLDO4_L2_V03]
    # cid['l2-hcho'] = "C2732717000-LARC_CLOUD"  # TEMPO Formaldehyde total column [TEMPO_HCHO_L2_V01]
    # cid['l2-hcho-v2'] = "C2842838927-LARC_CLOUD"  # TEMPO Formaldehyde total column [TEMPO_HCHO_L2_V02]
    if spec.lower()=='hcho':
        cid['l2-hcho-v3'] = "C2930730944-LARC_CLOUD"  # TEMPO Formaldehyde total column [TEMPO_HCHO_L2_V03]
        prod = 'TEMPO_HCHO_L2_V03'# cid['l2-no2'] = "C2724057189-LARC_CLOUD"  # TEMPO NO2 tropospheric, stratospheric, and total columns [TEMPO_NO2_L2_V01]
        # cid['l2-no2-v2'] = "C2842848994-LARC_CLOUD"  # TEMPO NO2 tropospheric, stratospheric, and total columns [TEMPO_NO2_L2_V02]
    elif spec.lower()=='no2':
        cid['l2-no2-v3'] = "C2930725014-LARC_CLOUD"  # TEMPO NO2 tropospheric, stratospheric, and total columns [TEMPO_NO2_L2_V03]
        prod = 'TEMPO_NO2_L2_V03'# cid['l2-o3tot'] = "C2724046381-LARC_CLOUD"  # TEMPO ozone total column [TEMPO_O3TOT_L2_V01]
        # cid['l2-o3tot-v2'] = "C2842849465-LARC_CLOUD"  # TEMPO ozone total column [TEMPO_O3TOT_L2_V02]
    elif spec.lower()=='o3':
        cid['l2-o3tot-v3'] = "C2930726639-LARC_CLOUD"  # TEMPO ozone total column [TEMPO_O3TOT_L2_V03]
        prod='TEMPO_O3TOT_L2_V03'
    # Level-3
    # cid['l3-cld'] = "C2724035076-LARC_CLOUD"  # TEMPO gridded cloud fraction and pressure (O2-O2 dimer) [TEMPO_CLDO4_L3_V01]
    # cid['l3-cld-v2'] = "C2842849693-LARC_CLOUD"  # TEMPO gridded cloud fraction and pressure (O2-O2 dimer) [TEMPO_CLDO4_L3_V02]
    # cid['l3-cld-v3'] = "C2930727817-LARC_CLOUD"  # TEMPO gridded cloud fraction and pressure (O2-O2 dimer) [TEMPO_CLDO4_L3_V03]
    # cid['l3-hcho'] = "C2724036159-LARC_CLOUD"  # TEMPO gridded formaldehyde total column [TEMPO_HCHO_L3_V01]
    # cid['l3-hcho-v2'] = "C2842849916-LARC_CLOUD"  # TEMPO gridded formaldehyde total column [TEMPO_HCHO_L3_V02]
    # cid['l3-hcho-v3'] = "C2930761273-LARC_CLOUD"  # TEMPO gridded formaldehyde total column [TEMPO_HCHO_L3_V03]
    # cid['l3-no2'] = "C2724036633-LARC_CLOUD"  # TEMPO gridded NO2 tropospheric, stratospheric, and total columns [TEMPO_NO2_L3_V01]
    # cid['l3-no2-v2'] = "C2842850219-LARC_CLOUD"  # TEMPO gridded NO2 tropospheric, stratospheric, and total columns [TEMPO_NO2_L3_V02]
    #cid['l3-no2-v3'] = "C2930763263-LARC_CLOUD"  # TEMPO gridded NO2 tropospheric, stratospheric, and total columns [TEMPO_NO2_L3_V03]
    # cid['l3-o3tot'] = "C2724037749-LARC_CLOUD"  # TEMPO gridded ozone total column [TEMPO_O3TOT_L3_V01]
    # cid['l3-o3tot-v2'] = "C2842852554-LARC_CLOUD"  # TEMPO gridded ozone total column [TEMPO_O3TOT_L3_V01]
    # cid['l3-o3tot-v2'] = "C2930764281-LARC_CLOUD"  # TEMPO gridded ozone total column [TEMPO_O3TOT_L3_V01]

    for date in dates:
        # For each product (keys in cid) search and retrieve (metadata and links for the chosen collection)

        start="{date:%Y-%m-%d} 00:00".format(date=date)
        end = "{date:%Y-%m-%d} 23:59".format(date=date)
        print("retrieving...", start, end)
        for k,v in cid.items():
            print(v)
            log_message('### Retrieving {}...'.format(k))
            results = earthaccess.search_data(
                concept_id=v,
                temporal = (start, end)
                # count = 200 # Restricting number of records returned
            )
            # From each result get the product name, year, month, day and form
            # the destination folder
            ndow = 0
            for r in results:
                # Limit number of downloads to max_down_files
                if ndow == max_down_files:
                    log_message('warning: skipping rest of files to download')
                    log_message('--->reached limit of downloads ({})'.format(max_down_files))
                    break
                #prod =  '_'.join((r['meta']['native-id'].split('_')[0:4]))
                year = r['meta']['native-id'].split('_')[4][0:4]
                month = r['meta']['native-id'].split('_')[4][4:6]
                day = r['meta']['native-id'].split('_')[4][6:8]
                dst_path = os.path.join(root,spec.lower(),ver,year,month,day)
                print(dst_path)
                dst_filename = os.path.join(dst_path,r['meta']['native-id'])
                # If the folder does not exists create it
                if not os.path.exists(dst_path):
                    os.makedirs(dst_path)
                # If file does not extist download it
                if not os.path.exists(dst_filename):
                    try:
                        log_message('downloading {}'.format(dst_filename))
                        downloaded_files = earthaccess.download(
                            r,
                            local_path=dst_path,
                            threads=1
                        )
                        ndow = ndow + 1
                        sleep(2)
                    except Exception as e:
                        log_message(e)
                        log_message('error downloadig {}'.format(dst_filename))
                        print('trying direct download instead')
                        filename =dst_filename.split('/')[-1]
                        os.chdir(dst_path)
                        print("wget --user debora.griffin --password 'DV08mkj!01234' https://data.asdc.earthdata.nasa.gov/asdc-prod-protected/TEMPO/{prod}/{date:%Y.%m.%d}/{filename}".format(prod=prod,date=date, filename=filename))
                        os.system("wget --user debora.griffin --password 'DV08mkj!01234' https://data.asdc.earthdata.nasa.gov/asdc-prod-protected/TEMPO/{prod}/{date:%Y.%m.%d}/{filename}".format(prod=prod,date=date, filename=filename))
                        os.chdir("/home/mas001/computer_programs/python/tempo/")
                # else:
                #     log_message('skip {} since it exists'.format(dst_filename))
