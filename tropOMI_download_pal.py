#! /usr/bin/env python
# -*- coding: utf-8 -*-


import os
import argparse
import datetime
import requests
import sys

import shutil
import warnings
try:
    # For Python 3.0 and later
    from urllib.request import urlopen
except ImportError:
    # Fall back to Python 2's urllib2
    from urllib2 import urlopen
import json
import glob
import netCDF4

from pystac import Catalog
from pystac_client import ItemSearch
import requests
import os

L2_CATALOG = "https://data-portal.s5p-pal.com/api/s5p-l2"
catalog = Catalog.from_file(L2_CATALOG)
endpoint = catalog.get_single_link("search").target

#TODO Remove this
#warnings.filterwarnings("ignore")
alg_map = {
    'no2': 'knmi',
    'ch4': 'sron',
    'co': 'sron',
    'hcho': 'bira',
    'o3': 'dlr',
    'so2': 'cobra',
    'aer_lh': 'knmi',
    'aer_ai': 'knmi',
    'o3_tcl': 'dlr',
    'no2omi': 'sp',
    'cloud': 'dlr',
    'np_bd3': 'ral',
    'np_bd6': 'ral',
    'np_bd7': 'ral',
    'aer_uv': 'nasa',
    'bro':'bira'
}
# TODO catch the occasional ConnectionError and retry (29)
#
# class Connection(object):
#     def __init__(self):
#         pass
#
#     def new_connect(self, url, urlp, user, password):
#         self.url = url
#         self.urlp = urlp
#         self.user = user
#         self.password = password
#         self.login = (self.user, self.password)
#
#     def connection_request(self):
#         with warnings.catch_warnings():
#             warnings.simplefilter("ignore")
#             self.query_r = requests.get(self.url, auth=(self.user, self.password))
#
#     def read_json(self):
#         self.json = self.query_r.json()['d']['results']
#
#     def get_file_id(self):
#         self.file_id = [(ent['Id'], ent['Name']) for ent in self.json]
#
#     def status(self):
#         return self.query_r.status_code
#
#     def connection(self):
#         return self.query_r
#
#     def number_file(self):
#         return len(self.file_id)

if __name__ == '__main__':

# -------------------------------------------------------------------------
# (0) Parse arguments
# -------------------------------------------------------------------------

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
        '-u', '--user',
        help='Your Copernicus hub username.'
    )
    parser.add_argument(
        '-p', '--password',
        help='Your Copernicus hub password'
    )

    parser.add_argument(
    '-n', '--num_version', default='None',
    help='version number you would like to download, '
         'use if multiple versions of RPRO exist, enter e.g.v010107'
    )
    parser.add_argument(
        '-hr', '--hour', nargs=2,
        required=False,
        type=int,
        metavar=('Min_h', 'Max_h'),
        default=[0, 23],
        help='version number you would like to download, '
             'use if multiple versions of RPRO exist, enter e.g.v010107'
    )
    parser.add_argument(
        '--tropdir', default=None,
        help='Directory where the tropOMI files will be stored. This should '
             'point to the format directory in the structure, and all sub-'
             'directories should be arranged in <YYYY>/'
    )
    parser.add_argument(
        '-v', '--version', required=False,
        help='Data product version.'
    )
    parser.add_argument(
        '--relaxed', action='store_true',
        help='If specified will simply ignore any days which error out or '
             'contain no files. Otherwise, an error will be raised.'
    )
    parser.add_argument(
        '--uncombined', action='store_true',
        help='Specify if you do not want to extract and combine the data files '
             'into global daily combined files. Otherwise files will be '
             'combined into daily files. See tropOMI_combine.py for details. '
             'You may wish to utilize this option if the combining is taking '
             'too long and being killed.'
    )
    parser.add_argument(
        '--verbose', action='store_true',
        help='Specify in order to output information about the progress of the '
             'downloads while the script is running.'
    )
    parser.add_argument(
        '--debug', action='store_true',
        help='Create a log file with information for use in debugging.'
    )
    args = parser.parse_args()

# -------------------------------------------------------------------------
# (1) Check all arguments
# -------------------------------------------------------------------------

    debug_flag = args.debug
    relax_flag = args.relaxed
    comb_flag = not args.uncombined
    load_flag = args.verbose
    num_version = args.num_version
    hh = args.hour

    hours = range(min(args.hour[:]), max(args.hour[:])+1)
    try:
        dates = trop_tool.daterange(args.daterange)
    except ValueError:
        parser.error("Invalid daterange. Please provide a range of two dates"
                     " in the form: YYYY.MM.DD or YYYY.DDD")

    spec = args.species.upper()
    ver = args.version if args.version else 'NRTI'  # TODO do much ver better (38)
    # TODO add version to directory structure? (45)
    if spec.lower()=='so2':
        tropdir = '{dir}/{spec}/{alg}/v2/nc/'.format(
            dir='/home/mas001/msh/satdata/level2/tropomi/s5p',
            spec=spec.lower(),
            alg=alg_map[spec.lower()],
            vers=ver
        ) if not args.tropdir else args.tropdir
    else:
        tropdir = '{dir}/{spec}/{alg}/{vers}/nc/'.format(
            dir='/home/mas001/msh/satdata/level2/tropomi/s5p',
            spec=spec.lower(),
            alg=alg_map[spec.lower()],
            vers=ver
        ) if not args.tropdir else args.tropdir


# ---------------------------------------------------------------------
# (1.5) Initiate debugging log file, if requested
# ---------------------------------------------------------------------

    if debug_flag:
        t0 = datetime.datetime.now()
        debug_dir = '{}/debug'.format(os.getcwd())
        logfile = '{dir}/log_download_c{c:%y%m%d%H%M%S}.log'.format(
            dir=debug_dir if os.path.exists(debug_dir) else os.getcwd(),
            c=t0
        )

        mssg = '{d} tropOMI FILE DOWNLOAD {d}\n\n'.format(d='-' * 10)
        for n, a in vars(args).items():
            mssg += '{0}: {1}\n'.format(n, a)
        trop_tool.debug(logfile, mssg)

# -------------------------------------------------------------------------
# (2) Query database to get UUID's
# -------------------------------------------------------------------------

    if load_flag:
        import pyloader
        load = pyloader.Load_message('')

    for date in dates:
        if load_flag:
            load.update('> {d:%Y.%j} | Retrieving file list'.format(d=date))

        # ------------------------------------------------------------------
        # Create all relevant directories
        # ------------------------------------------------------------------

        datedir = '{dir}/{dt:%Y/%j}'.format(dir=tropdir, dt=date)
        if not os.path.exists(datedir[:-4]):
            os.mkdir(datedir[:-4])
        if not os.path.exists(datedir):
            os.mkdir(datedir)
        os.chdir(datedir)
        print(datedir)
        if spec.lower()=='so2':
            timefilter = "{dt:%Y-%m-%d}".format(dt=date)
            items = list(ItemSearch(endpoint, filter="s5p:file_type='L2__SO2CBR'", datetime=timefilter).items())
            print(items)
            for item in items:
                product = item.assets["product"]
                extra_fields = product.extra_fields

                download_url = product.href
                product_local_path = extra_fields["file:local_path"]
                product_size = extra_fields["file:size"]

                print(f"Downloading {product_local_path}...")
                r = requests.get(download_url)
                with open(f"./{product_local_path}", "wb") as product_file:
                    product_file.write(r.content)
                print("Comparing file sizes...")
                file_size = os.path.getsize(f"./{product_local_path}")
                print(f"{file_size=}")
                assert file_size == product_size
                print("Sizes match; product was downloaded correctly")
        if spec.lower()=='bro':
            timefilter = "{dt:%Y-%m-%d}".format(dt=date)
            items = list(ItemSearch(endpoint, filter="s5p:file_type='L2__BRO___'", datetime=timefilter).items())
            print(items)
            for item in items:
                product = item.assets["product"]
                extra_fields = product.extra_fields

                download_url = product.href
                product_local_path = extra_fields["file:local_path"]
                product_size = extra_fields["file:size"]

                print(f"Downloading {product_local_path}...")
                r = requests.get(download_url)
                with open(f"./{product_local_path}", "wb") as product_file:
                    product_file.write(r.content)
                print("Comparing file sizes...")
                file_size = os.path.getsize(f"./{product_local_path}")
                print(f"{file_size=}")
                assert file_size == product_size
                print("Sizes match; product was downloaded correctly")
        # filenames = glob.glob(datedir+'/*.nc')
        # for files in filenames:
        #     try:
        #         nc = netCDF4.Dataset(files)
        #         nc.close()
        #     except:
        #         os.system('rm %s'%files)
        #         print('rm %s'%files)
        #
        # if debug_flag:
        #     mssg = '\nDownloading day: {d:%Y-%m-%d} ---------\n\n'.format(d=date)
        #     mssg += 'date directory: {}\n'.format(datedir)
        #     trop_tool.debug(logfile, mssg)
        #
        # ------------------------------------------------------------------
        # Utilize the cophub filtering api to create a  which lists, in
        # json format, all files which exist on this date with the corrrect
        # version and species in the filename.
        # See https://cophub.copernicus.eu/twiki/do/view/SciHubUserGuide
        # /5APIsAndBatchScripting for more details
        # ------------------------------------------------------------------
        # the top filter is a maximum of 100 files, there are typically more than 100 files for NRTI data -> need to loop over hours
        # if spec.lower()=='no2':
        #     url = 'https://data-portal.s5p-pal.com/cat/sentinel-5p/S5P_L2__NO2___/{date:%Y}/{date:%m}/{date:%d}/catalog.json'.format(date = date)
        # elif spec.lower()=='so2':
        #     url = 'https://data-portal.s5p-pal.com/api/s5p-l2/L2__SO2CBR/{date:%Y}/{date:%-m}/{date:%d}/collection.json'.format(date=date)
        #     print(url)
        # elif spec.lower()=='bro':
        #     url = 'https://data-portal.s5p-pal.com/api/s5p-l2/L2__BRO___/{date:%Y}/{date:%-m}/{date:%d}/collection.json'.format(date=date)
        #     print(url)
        # jsonurl = urlopen(url)
        # items = json.loads(jsonurl.read())
        #
        #
        # links = items['links']
        # print('shape', links)
        # download_url = links[0]["href"]
        # product_filename = links[0]["title"]
        # print('downlaod url',download_url)
        # print ('name', product_filename)
        # for ind in range(4,len(links)):
        #     print(ind)
        #     download = links[ind]["href"]
        #     print(download)
        #     filename = links[ind]["title"]
        #     #load the json again
        #     jsonurl2 = urlopen(download)
        #     items2 = json.loads(jsonurl2.read())
        #     assets = items2['assets']
        #     print('assets',assets)
        #     dw = assets['download']
        #     print('dw',dw)
        #     UUID = dw['href']
        #     print('UUID', UUID)
        #     localpath = '{dir}/{file}.nc'.format(dir=datedir, file=filename)
        #     print('downloading to...', localpath)
        #     #print(localpath)
        #     if not os.path.exists(localpath) or not os.path.getsize(localpath):
        #
        #         # print 'here, request', f
        #         # #try wget
        #         # command = "wget --no-check-certificate --continue --user=s5pguest --password=s5pguest %s -P %s"%(f, datedir)
        #         # print command
        #         # os.system(command)
        #         import time
        #
        #         file_r = ''
        #         while file_r == '':
        #             try:
        #                 file_r = requests.get(UUID)
        #                 break
        #             except:
        #                 print("Connection refused by the server..")
        #                 print("Let me sleep for 5 seconds")
        #                 print("ZZzzzz...")
        #                 time.sleep(5)
        #                 print("Was a nice sleep, now let me continue...")
        #                 continue
        #         # file_r = requests.get(UUID)
        #         # file_r.raise_for_status()
        #
        #
        #
        #     # --------------------------------------------------------------
        #     # Write the data locally, in 1024 chunks
        #     # --------------------------------------------------------------
        #
        #
        #     if not os.path.exists(localpath) or not os.path.getsize(localpath):
        #         with open(localpath, 'wb') as f:
        #             f.write(file_r.content)
        #             # try:
        #             #     for chunk in file_r.iter_content(chunk_size=None):
        #             #
        #             #         if chunk:
        #             #             f.write(chunk)
        #             # except:
        #             #     continue

