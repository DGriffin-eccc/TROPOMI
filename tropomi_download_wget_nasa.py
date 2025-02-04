# Import Python packages
# need to create an account on GES DISC earthdata, for details see: https://disc.gsfc.nasa.gov/information/documents?title=Data%20Access 
# to run simply use the command in your terminal, e.g. (change the date,species, version (OFFL/RPRO), and directory, Earthdata password and username accordingly) :
# python tropomi_download_wget_nasa.pt -d 2024.01.01. 2024.01.02 -v OFFL -s NO2 --tropdir path_to_your_directory --user username --passw password
# Module for manipulating dates and times
import datetime

# Module to set filesystem paths appropriate for user's operating system
from pathlib import Path

import os
import argparse
import datetime
import requests
import sys
sys.path.append('/home/mas001/computer_programs/python/tropOMI/')

import shutil
import warnings
import subprocess
import re
import numpy as np
import netCDF4
# Get product abbrevation used in TROPOMI file name
# "product": parameter variable from widget menu, set in main function
alg_map = {
    'no2': 'knmi',
    'ch4': 'sron',
    'co': 'sron',
    'hcho': 'bira',
    'o3': 'dlr',
    'so2': 'dlr',
    'aer_lh': 'knmi',
    'aer_ai': 'knmi',
    'o3_tcl': 'dlr',
    'no2omi': 'sp',
    'cloud': 'dlr',
    'np_bd3': 'ral',
    'np_bd6': 'ral',
    'np_bd7': 'ral',
    'aer_uv': 'nasa'
}
def def_daterange(argdates):
    '''*Given a start and end string date, return a range of datetimes*

    Given a two-tuple of string dates in the format YYYY.MM.DD or YYYY.DDD, one
    for the start and end dates of a range, will return a list containing a
    python datetime object for each date in said range.

    Args
    ----
    argdates : tuple of str
        Tuple containing a beginning and ending date, in the format `YYYY.MM.DD`
        or `YYYY.DDD`. Both dates must be in the same format

    Returns
    -------
    dates : list of datetime.datetime
        List containing datetimes for each date within the range provided.

    Raises
    ------
    ValueError: If date string format is given incorrectly or invalid date
        range is given.

    '''
    import datetime

    # ----------------------------------------------------------------------
    # Read the string dates to datetime object, in either format
    # ----------------------------------------------------------------------

    if type(argdates[0]) is not str:
        for ind in len(argdates):
            argdates[ind] = str(argdates)
    try:
        frm = '%Y.%m.%d'
        argdts = [datetime.datetime.strptime(d, frm) for d in argdates]
    except ValueError:
        frm = '%Y.%j'
        for date in argdates: print(date);datetime.datetime.strptime(str(date), frm)
        argdts = [datetime.datetime.strptime(d, frm) for d in argdates]

    if not argdts:
        mssg = 'no dates provided'
        raise ValueError(mssg)

    if len(argdts) > 2:
        mssg = 'provided more than 2 dates to date range'
        raise ValueError(mssg)

    if len(argdts) == 2 and argdts[0] > argdts[1]:
        mssg = 'start date must come after end date'
        raise ValueError(mssg)

    # ----------------------------------------------------------------------
    # Iterate one day at a time throughout the date range and create
    # datetime objects for each day
    # ----------------------------------------------------------------------

    s = argdts[0]
    dates = []
    while s <= argdts[-1]:
        dates.append(s)
        s += datetime.timedelta(days=1)

    return dates


def extract_href_and_title(input_file):
    with open(input_file, 'r') as file:
        content = file.read()

    href_pattern = r'<link href="(.*?)"'
    title_pattern = r'<title>S5P(.*?)</title>'

    href_matches = re.findall(href_pattern, content)
    title_matches = re.findall(title_pattern, content)

    return list(zip(href_matches, title_matches))


def get_tropomi_product_abbreviation(product):

    if product == 'CO':
        product_abbreviation = 'S5P_L2__CO_____HiR.2/'
    elif product == 'NO2':
        product_abbreviation = 'S5P_L2__NO2____HiR.2/'
    elif product == 'SO2':
        product_abbreviation = 'S5P_L2__SO2____HiR.2/'
    elif product == 'HCHO':
        product_abbreviation = 'S5P_L2__HCHO___HiR.2/'
    elif product == 'AER_AI':
        product_abbreviation = 'S5P_L2__AER_AI_HiR.2/'
    elif product == 'AER_LH':
        product_abbreviation = 'S5P_L2__AER_LH_HiR.2/'
    elif product == "O3":
        product_abbreviation = "S5P_L2__O3_TOT_HiR.2/"
    elif product == "CH4":
        product_abbreviation = "S5P_L2__CH4____HiR.2/"
    elif product == "CLOUD":
        product_abbreviation = "S5P_L2__CLOUD__HiR.2/"
    return product_abbreviation

def get_tropomi_product_version(product):
    if product == 'CO':
        product_abbreviation = "020400"
    elif product == 'NO2':
        product_abbreviation = '020400'
    elif product == 'SO2':
        product_abbreviation = '020401'
    elif product == 'HCHO':
        product_abbreviation = '020401'
    elif product == 'AER_AI':
        product_abbreviation = '020400'
    elif product == 'AER_LH':
        product_abbreviation = '020400'
    elif product == "O3":
        product_abbreviation = "020401"
    elif product == "CH4":
        product_abbreviation = '020400'
    elif product == "CLOUD":
        product_abbreviation = "020401"
    return product_abbreviation

def get_file_list(self, ext=None):
    """ Get file list that end in .he5"""

    print("Finding list of files...")

    # Finds all files that end in .he5
    # re has weird syntax look it up if
    # you want to understand it
    if not ext:
        ext='nc'

    # Demonstration of the regex method:
    # https://bit.ly/2F5vuAU
    self.file_list = re.findall(
         r'[\w\.-]+\w.%s'%(ext),
         self.folder_cont
    )
    print(self.file_list)
    print("Found list of files")
    # Checks to see if any file was listed twice
    self.file_list = np.unique(self.file_list)
    print(self.file_list)
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Find a specific feature type, within a coord/date range, "
                    "in the CALIOP VFM product."
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
             'the tropOMI data file name.'
    )

    parser.add_argument(
        '--tropdir', default=None,
        help='Directory where the tropOMI files will be stored. This should '
             'point to the format directory in the structure, and all sub-'
             'directories should be arranged in <YYYY>/'
    )
    parser.add_argument(
        '--user', default=None,
        help='GES DISC username'
    )
    parser.add_argument(
        '--passw', default=None,
        help='GES DISC password'
    )
    parser.add_argument(
        '-v', '--version', required=False,
        help='Data product version.'
    )

    args = parser.parse_args()
    server = 'https://tropomi.gesdisc.eosdis.nasa.gov/data/S5P_TROPOMI_Level2/'
    try:
        dates = def_daterange(args.daterange)
    except ValueError:
        parser.error("Invalid daterange. Please provide a range of two dates"
                     " in the form: YYYY.MM.DD or YYYY.DDD")

    spec = args.species.upper()
    ver = args.version.upper() #if args.version else 'NRTI'  # TODO do much ver better (38)
    latency = {'NRTI':'Near real time',
               'OFFL':'Offline',
               'RPRO':'Reprocessing'}
    
    tropdir = '{dir}/{spec}/{alg}/nc/{vers}'.format(
        dir='/home/mas001/msh/satdata/level2/tropomi/s5p',
        spec=spec.lower(),
        alg=alg_map[spec.lower()],
        vers=ver.upper()
    ) if not args.tropdir else args.tropdir

    # Set directory to save downloaded files (as pathlib.Path object)
    # loop over dates
    for date in dates:
        day = '{dt:%Y-%m-%d}'.format(dt=date)
        datedir = '{dir}/{dt:%Y/%j}'.format(dir=tropdir, dt=date)
        if not os.path.exists(datedir[:-4]):
            os.mkdir(datedir[:-4])
        if not os.path.exists(datedir):
            os.mkdir(datedir)
        save_path = datedir
        os.chdir(save_path)

        # Get TROPOMI product abbreviation used in the file name
        product_abbreviation = get_tropomi_product_abbreviation(spec.upper())
        host = server+product_abbreviation
        #process_number = get_tropomi_product_version(spec.upper())
        print('download all')
        http_url = '{host}{year}/{doy}/'.format(
            host=host,
            year=str(date.strftime("%Y")),
            doy=str(date.strftime("%j")))
        print("Downloading files")
        print('wget -q -nH -nd %s -O - | grep nc | cut -f4 -d\\" > filelist.txt'%http_url)
        os.system('wget -q -nH -nd %s -O - | grep nc | cut -f4 -d\\" > filelist.txt'%http_url)

        with open('filelist.txt', "r") as file:
            lines = file.readlines()
        #file_list = lines[2:]
        print(lines)
        #file_list = [file for file in lines if '_03_' in file]
        file_list = [file for file in lines if ver.upper() in file]

        file_list = [file for file in file_list if  file.endswith('.nc\n')]
        file_list = [file.rstrip('\n') for file in file_list]
        print(file_list)
        for data_file in file_list:
            file_url = '{url}{file}'.format(url=http_url, file=data_file)
            print(file_url)
            localpath = '{dir}/{file}'.format(dir=save_path, file=data_file)
            print(
                "wget -nc %s -O %s" % (
                file_url,localpath))
            os.system(
                "wget -nc --user %s --password %s  %s -O %s" % (args.user, args.passw,
                file_url, localpath))
        os.system('rm filelist.txt')

        print("=== Downloading TROPOMI Data ===")
        start_time = datetime.datetime.now()
        print(save_path)

