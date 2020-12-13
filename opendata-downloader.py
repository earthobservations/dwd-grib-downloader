#!/usr/bin/env python
""" opendata-downloader.py

 Script to download and extract grib files from DWD's open data file server https://opendata.dwd.de

 Author:
    Eduard Rosert
 Version history:
    0.2, 2019-10-17, added --get-latest-timestamp, --min-timestamp option
    0.1, 2019-10-01, initial version
"""

try:
    import argparse
    import sys
    import csv
    import urllib.request
    import bz2
    import json
    import math
    import os
    from datetime import datetime, timedelta, timezone
    import logging as log
    from extendedformatter import ExtendedFormatter
except ImportError as ie:
    print("required libraries could not be found:")
    print(ie)
    sys.exit(1)

global dryRun
global compressed
global skipExisting
skipExisting = True
dryRun = None
compressed = False

# custom stringFormatter with uppercase/lowercase functionality
stringFormatter = ExtendedFormatter()
supportedModels = {}
with open("models.json", "r") as jsonfile:
    models = json.load(jsonfile)
    for model in models:
        supportedModels[model["model"]] = model


def configureHttpProxyForUrllib(proxySettings={'http': 'proxyserver:8080'}):
    proxy = urllib.request.ProxyHandler(proxySettings)
    opener = urllib.request.build_opener(proxy)
    urllib.request.install_opener(opener)


def getMostRecentModelTimestamp(waitTimeMinutes=360, modelIntervalHours=3, modelrun=None):

    # explicit model run timestamp
    if modelrun:
        return datetime.strptime(modelrun, '%Y%m%d%H')


    # model data becomes available approx 1.5 hours (90minutes) after a model run
    # cosmo-d2 model and icon-eu run every 3 hours
    now = datetime.utcnow() - timedelta(minutes=waitTimeMinutes)

    latestAvailableUTCRun = int(math.floor(
        now.hour / modelIntervalHours) * modelIntervalHours)
    modelTimestamp = datetime(
        now.year, now.month, now.day, latestAvailableUTCRun)
    return modelTimestamp


def downloadAndExtractBz2FileFromUrl(url, destFilePath=None, destFileName=None):
    if dryRun:
        log.info("dryrun: '{0}'".format(url))
        return
    log.info("downloading file: '{0}'".format(url))

    if destFileName == "" or destFileName == None:
        # strip the filename from the url and remove the bz2 extension
        destFileName = url.split('/')[-1].split('.bz2')[0]

    if destFilePath == "" or destFilePath == None:
        destFilePath = os.getcwd()

    if compressed:
        fullFilePath = os.path.join(destFilePath, destFileName + '.bz2')
    else:
        fullFilePath = os.path.join(destFilePath, destFileName)
    if skipExisting and os.path.exists(fullFilePath):
        log.info("skipping existing file: '{0}'".format(fullFilePath))
        return
    
    resource = urllib.request.urlopen(url)
    compressedData = resource.read()
    if compressed:
        binaryData = compressedData
    else:
        binaryData = bz2.decompress(compressedData)

    log.info("saving file as: '{0}'".format(fullFilePath))
    with open(fullFilePath, 'wb') as outfile:
        outfile.write(binaryData)
    log.info("Done.")


# icon-d2/grib/12/u/icon-d2_germany_regular-lat-lon_single-level_2020120312_000_2d_u.grib2.bz2'
#  icon-d2/grib/12/u/icon-d2_germany_regular-lat-lon_model-level_2020120212_013_39_u.grib2.bz2
# https://opendata.dwd.de/weather/nwp/icon-d2/grib/12/u/icon-d2_germany_regular-lat-lon_model-level_2020120212_013_39_u.grib2.bz2
#                                                       icon-d2_germany_regular-lat-lon_model-level_2020120312_000_2d_u.grib2.bz2'
#                                                       icon-d2_germany_regular-lat-lon_model-level_2020120312_000_42_u.grib2.bz2'
#          "model-level":
#          "https://opendata.dwd.de/weather/nwp/{model!L}/grib/{modelrun:>02d}/{param!L}/{model!L}_{scope}_{grid}_{levtype}_{timestamp:%Y%m%d}{modelrun:>02d}_{step:>03d}_{level:>02d}_{param!L}.grib2.bz2",

def getGribFileUrl(model="icon-eu",
                   grid=None,
                   param="t_2m",
                   timestep=0,
                   timestamp=getMostRecentModelTimestamp(
                       waitTimeMinutes=180, modelIntervalHours=12),
                   levtype="single-level",
                   level=42):
    cfg = supportedModels[model]
    grid = "regular-lat-lon"
    if (grid is None) or (grid not in cfg["grids"]):
        grid = cfg["grids"][0]
    url = cfg["pattern"][levtype]
    # pattern is something like this:
    #  "https://opendata.dwd.de/weather/nwp/{model!l}/grib/{modelrun:>02d}/{param!l}
    # /{model!l}_{scope}_{grid}_{levtype}_{timestamp:%Y%m%d}{modelrun:>02d}_{step:>03d}_{param!u}.grib2.bz2"
    # level:
    # {model!L}_{scope}_{grid}_{levtype}_{timestamp:%Y%m%d}{modelrun:>02d}_{step:>03d}_{level:>d}_{param!L}.grib2.bz2"
    # e.g. https://opendata.dwd.de/weather/nwp/icon/grib/09/t_2m/icon_global_icosahedral_single-level_2020062609_000_T_2M.grib2.bz2
    #                                                     icon-d2_germany_regular-lat-lon_model-level_2020120312_000_00_v.grib2.bz2'
    #                                                     icon-d2_germany_regular-lat-lon_model-level_2020120312_000_42_u.grib2
    return stringFormatter.format(url,
                                  model=cfg["model"],
                                  param=param,
                                  grid=grid,
                                  modelrun=timestamp.hour,
                                  scope=cfg["scope"],
                                  levtype=levtype,
                                  timestamp=timestamp,
                                  step=timestep,
                                  level=level)


def downloadGribData(model="icon-eu",
                     grid=None,
                     param="t_2m",
                     timestep=0,
                     timestamp=getMostRecentModelTimestamp(),
                     destFilePath=None,
                     destFileName=None,
                     level=0,
                     levtype="single-level"):


    dfp = destFilePath
    cfg = supportedModels[model]
    if "destpattern" in cfg:
        pat = cfg["destpattern"]
        # replicate DWD tree like https://opendata.dwd.de/weather/nwp/icon-d2/grib/00/t_2m/
        subdir = stringFormatter.format(pat,
                                        model=model,
                                        param=param,
                                        grid=grid,
                                        modelrun=timestamp.hour,
                                        levtype=levtype,
                                        timestamp=timestamp,
                                        step=timestep,
                                        level=level)
        dfp = os.path.join( destFilePath, subdir)
    if not os.path.exists(dfp):
        if dryRun:
            log.debug(f"mdkir {dfp}")
        else:
            os.makedirs(dfp)

    log.debug(f"destFilePath={dfp}")
    dataUrl = getGribFileUrl(model=model, grid=None, param=param,
                             timestep=timestep, timestamp=timestamp, level=level, levtype=levtype)
    downloadAndExtractBz2FileFromUrl(
        dataUrl, destFilePath=dfp, destFileName=destFileName)


def downloadGribDataSequence(model="icon-eu",
                             grid=None,
                             param="t_2m",
                             minTimeStep=0,
                             maxTimeStep=12,
                             minModelLevel=0,
                             maxModelLevel=0,
                             levtype="single-level",
                             timestamp=getMostRecentModelTimestamp(),
                             destFilePath=None):
    # download data from open data server for the next x steps
    for timestep in range(minTimeStep, maxTimeStep + 1):
        for l in range(minModelLevel, maxModelLevel + 1):
            downloadGribData(model=model, grid=None, param=param, timestep=timestep,
                             timestamp=timestamp, destFilePath=destFilePath,
                             level=l,
                             levtype=levtype)


def formatDateIso8601(date):
    return date.replace(microsecond=0, tzinfo=timezone.utc).isoformat()


def getTimestampString(date):
    modelrun = "{0:02d}".format(date.hour)
    return date.strftime("%Y%m%d" + modelrun)


parser = argparse.ArgumentParser(
    description='A tool to download grib model data from DWD\'s open data server https://opendata.dwd.de .',
    add_help=True)

parser.add_argument('--model', choices=supportedModels.keys(),
                    dest='model',
                    type=str,
                    required=True,
                    help='the model name')

parser.add_argument('--grid', choices=["icosahedral", "regular-lat-lon", "rotated-lat-lon"],
                    dest='grid',
                    type=str,
                    required=False,
                    default=None,
                    help='the grid type')

parser.add_argument('--get-latest-timestamp',
                    dest='getLatestTimestamp',
                    action='store_true',
                    help='Returns the latest available timestamp for the specified model.')


# use it like this: --single-level-fields t_2m pmsl clch ...
parser.add_argument('--single-level-fields',
                    dest='params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=None,  # ['t_2m'],
                    help='one or more single-level model fields that should be donwloaded, e.g. t_2m, tmax_2m, clch, pmsl, ...')

# use it like this: --single-level-fields t_2m pmsl clch ...
parser.add_argument('--model-level-fields',
                    dest='level_params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=None,  # ['t_2m'],
                    help='one or more model-level fields that should be donwloaded, e.g. u, v, p, m, ...')

# use it like this: --single-level-fields t_2m pmsl clch ...
parser.add_argument('--time-invariant-fields',
                    dest='time_invariant_params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=None,  # ['t_2m'],
                    help='one or more time invariant fields that should be donwloaded, e.g. hhl, ...')

parser.add_argument('--min-model-level', dest='minModelLevel', default=0, type=int,
                    help='the minimum level number to download (default=0)')

parser.add_argument('--max-model-level', dest='maxModelLevel', default=0,  # 90,
                    type=int,
                    help='the maximum level number to download (default=0)')

parser.add_argument('--min-time-step', dest='minTimeStep', default=0, type=int,
                    help='the minimum forecast time step to download (default=0)')

parser.add_argument('--max-time-step', dest='maxTimeStep', default=-1, type=int,
                    help='the maximung forecast time step to download, e.g. 12 will download time steps from min-time-step - 12. If no max-time-step was defined, no data will be downloaded.')

parser.add_argument('--directory', dest='destFilePath', default=os.getcwd(),
                    help='the download directory')

parser.add_argument('--modelrun', dest='modelrun', default=None,
                    help='explicitly download from a particular model run. Example: --modelrun 2020121212')

parser.add_argument('--http-proxy', dest='proxy', metavar='proxy_name_or_ip:port', required=False,
                    help='the http proxy url and port')

parser.add_argument("-v", "--verbose", help="increase output verbosity",
                    action="store_const", dest="loglevel", const=log.INFO)

parser.add_argument("-d", "--dry-run", help="only show debug output, do not download",
                    action="store_true", dest="dryRun")

parser.add_argument("-c", "--compressed", help="store as bz2 file (do not uncompress)",
                    action="store_true", dest="compressed")
parser.add_argument("-r", "--reload", help="reload files even if bz2 file exists - default to skipping existing files",
                    action="store_false", default=True, dest="skipexisting")
"""
usage: opendata-downloader.py [-h] --model
                              {cosmo-d2,cosmo-d2-eps,icon,icon-eps,icon-eu,icon-eu-eps,icon-d2,icon-d2-eps}
                              [--grid {icosahedral,regular-lat-lon,rotated-lat-lon}]
                              [--get-latest-timestamp]
                              [--single-level-fields shortName [shortName ...]]
                              [--min-time-step MINTIMESTEP]
                              [--max-time-step MAXTIMESTEP]
                              [--directory DESTFILEPATH]
                              [--http-proxy proxy_name_or_ip:port] [-v]

A tool to download grib model data from DWD's open data server
https://opendata.dwd.de .

optional arguments:
  -h, --help            show this help message and exit
  --model {cosmo-d2,cosmo-d2-eps,icon,icon-eps,icon-eu,icon-eu-eps,icon-d2,icon-d2-eps}
                        the model name
  --grid {icosahedral,regular-lat-lon,rotated-lat-lon}
                        the grid type
  --get-latest-timestamp
                        Returns the latest available timestamp for the
                        specified model.
  --single-level-fields shortName [shortName ...]
                        one or more single-level model fields that should be
                        donwloaded, e.g. t_2m, tmax_2m, clch, pmsl, ...
  --min-time-step MINTIMESTEP
                        the minimum forecast time step to download (default=0)
  --max-time-step MAXTIMESTEP
                        the maximung forecast time step to download, e.g. 12
                        will download time steps from min-time-step - 12. If
                        no max-time-step was defined, no data will be
                        downloaded.
  --directory DESTFILEPATH
                        the download directory
  --http-proxy proxy_name_or_ip:port
                        the http proxy url and port
  -v, --verbose         increase output verbosity
"""
if __name__ == "__main__":
    logformat = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"

    args = parser.parse_args()
    if args.loglevel:
        log.basicConfig(format=logformat, level=log.DEBUG)  # verbose
    else:
        log.basicConfig(format=logformat, level=log.ERROR)  # default

    dryRun = args.dryRun
    compressed = args.compressed
    skipExisting = args.skipexisting

    if args.proxy:
        # configure proxy
        configureHttpProxyForUrllib(proxySettings={'http': args.proxy})

    # add custom dialect for csv export
    csv.register_dialect('excel-semicolon', delimiter=';',
                         quoting=csv.QUOTE_ALL, lineterminator='\r\n')

    # wait 5 hrs (=300 minutes) after a model run for icon-eu data
    # and 1,5 hrs (=90 minute) for cosmo-d2, just to be sure
    selectedModel = supportedModels[args.model.lower()]
    openDataDeliveryOffsetMinutes = selectedModel["openDataDeliveryOffsetMinutes"]
    modelIntervalHours = selectedModel["intervalHours"]
    latestTimestamp = getMostRecentModelTimestamp(
        waitTimeMinutes=openDataDeliveryOffsetMinutes, modelIntervalHours=modelIntervalHours, modelrun=args.modelrun)

    if args.getLatestTimestamp:
        print(getTimestampString(latestTimestamp))
        sys.exit(0)

    if args.params is None and args.level_params is None and args.time_invariant_params is None:
        log.error(
            "nothing to download. Specify either --single-level-fields <fields> or --model-level-fields <fields>")
        sys.exit(1)

    # download data
    if args.params:
        for param in args.params:
            downloadGribDataSequence(model=selectedModel["model"],
                                     grid=args.grid,
                                     param=param,
                                     minTimeStep=args.minTimeStep,
                                     maxTimeStep=args.maxTimeStep,
                                     timestamp=latestTimestamp,
                                     levtype="single-level",
                                     destFilePath=args.destFilePath)
    if args.level_params:
        for param in args.level_params:
            minModelLevel = args.minModelLevel if args.minModelLevel > 0 else selectedModel[
                "minlevel"]
            maxModelLevel = args.maxModelLevel if args.maxModelLevel > 0 else selectedModel[
                "maxlevel"]
            downloadGribDataSequence(model=selectedModel["model"],
                                     grid=args.grid,
                                     param=param,
                                     minTimeStep=args.minTimeStep,
                                     maxTimeStep=args.maxTimeStep,
                                     timestamp=latestTimestamp,
                                     minModelLevel=minModelLevel,
                                     maxModelLevel=maxModelLevel,
                                     levtype="model-level",
                                     destFilePath=args.destFilePath)

    if args.time_invariant_params:
        for param in args.time_invariant_params:
            minModelLevel = args.minModelLevel if args.minModelLevel > 0 else selectedModel[
                "minlevel"]
            maxModelLevel = args.maxModelLevel if args.maxModelLevel > 0 else selectedModel[
                "maxlevel"]
            downloadGribDataSequence(model=selectedModel["model"],
                                     grid=args.grid,
                                     param=param,
                                     minTimeStep=args.minTimeStep,
                                     maxTimeStep=args.maxTimeStep,
                                     timestamp=latestTimestamp,
                                     minModelLevel=minModelLevel,
                                     maxModelLevel=maxModelLevel,
                                     levtype="time-invariant",
                                     destFilePath=args.destFilePath)
