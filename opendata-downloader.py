#!/usr/bin/env python
""" opendata-downloader.py

 Script to download and extract grib files from DWD's open data file server https://opendata.dwd.de

 original Author:
    Eduard Rosert
 extensive rewrite:
    Michael Haberler
 Version history:
    x.y, 2020-12-22, parallelize download, support model-level pressure-level time-invariant on some models
    0.2, 2019-10-17, added --get-latest-timestamp, --min-timestamp option
    0.1, 2019-10-01, initial version
"""

try:
    import argparse
    import sys
    import csv
    import urllib.request
    from urllib.error import URLError, HTTPError
    import bz2
    import json
    import math
    import os
    from datetime import datetime, timedelta, timezone
    import logging as log
    from extendedformatter import ExtendedFormatter
    import concurrent.futures
    from concurrent.futures.thread import ThreadPoolExecutor

except ImportError as ie:
    log.exception("Importing required libraries failed")
    sys.exit(1)

global dryRun
global compressed
global skipExisting
global maxWorkers
skipExisting = True
dryRun = None
compressed = False
retainDwdTree = True
dwdPattern = "{model!L}/grib/{modelrun:>02d}/{param!L}"
failedFiles = []


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
        log.debug("Pretending to download file: '{0}' (dry-run)".format(url))
        return
    else:
        log.debug("Downloading file: '{0}'".format(url))

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
        log.debug("Skipping existing file: '{0}'".format(fullFilePath))
        return fullFilePath

    try:
        resource = urllib.request.urlopen(url)
        compressedData = resource.read()
        if compressed:
            binaryData = compressedData
        else:
            binaryData = bz2.decompress(compressedData)

        log.debug("Saving file as: '{0}'".format(fullFilePath))
        with open(fullFilePath, 'wb') as outfile:
            outfile.write(binaryData)
        return fullFilePath
    except HTTPError as e:
        log.error(f"Downloading failed. Reason={e}, URL={url}")
        failedFiles.append((url, e.status, HTTPError))
    except Exception as e:
        log.exception(f"Downloading failed. Reason={e}, URL={url}")

def getGribFileUrl(model="icon-eu",
                   grid=None,
                   param="t_2m",
                   timestep=0,
                   timestamp=getMostRecentModelTimestamp(
                       waitTimeMinutes=180,
                       modelIntervalHours=12),
                   levtype="single-level",
                   level=42):

    cfg = supportedModels[model]

    # When "grid" parameter is not given, use first available grid type.
    if (grid is None) or (grid not in cfg["grids"]):
        grid = cfg["grids"][0]

    url = cfg["pattern"][levtype]
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

    dataUrl = getGribFileUrl(model=model,
                             grid=grid,
                             param=param,
                             timestep=timestep,
                             timestamp=timestamp,
                             level=level,
                             levtype=levtype)

    log.info(f"Downloading {dataUrl}")
    output_file = downloadAndExtractBz2FileFromUrl(dataUrl,
                                     destFilePath=destFilePath,
                                     destFileName=destFileName)
    return {"url": dataUrl, "file": output_file}


def downloadGribDataSequence(model="icon-eu",
                             flat=False,
                             grid=None,
                             param="t_2m",
                             timeSteps=[],
                             levelRange=[],
                             levtype="single-level",
                             timestamp=getMostRecentModelTimestamp(),
                             destFilePath=None):
    dfp = destFilePath
    cfg = supportedModels[model]
    if not flat:
        # replicate DWD tree like
        # https://opendata.dwd.de/weather/nwp/icon-d2/grib/00/t_2m/
        subdir = stringFormatter.format(dwdPattern,
                                        model=model,
                                        param=param,
                                        grid=grid,
                                        modelrun=timestamp.hour,
                                        levtype=levtype,
                                        timestamp=timestamp)
        dfp = os.path.join(destFilePath, subdir)

    if not os.path.exists(dfp):
        if dryRun:
            log.debug(f"Creating directory: {dfp}")
        else:
            os.makedirs(dfp)

    log.info(f"Using {maxWorkers} workers for downloading")

    results = []
    with ThreadPoolExecutor(max_workers=maxWorkers) as executor:
        futures = []
        for timestep in timeSteps:
            for level in levelRange:
                futures.append(executor.submit(downloadGribData,
                                               model=model,
                                               grid=grid,
                                               param=param,
                                               timestep=timestep,
                                               timestamp=timestamp,
                                               destFilePath=dfp,
                                               level=level,
                                               levtype=levtype))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            log.debug("Result: {}".format(result))

    return results


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

parser.add_argument('--single-level-fields',
                    dest='single_level_params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=[],
                    help='one or more single-level model fields that should be downloaded, e.g. t_2m, tmax_2m, clch, pmsl, ...')

parser.add_argument('--model-level-fields',
                    dest='model_level_params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=[],  # ['t_2m'],
                    help='one or more model-level fields that should be downloaded, e.g. u, v, p, m, ...')

parser.add_argument('--pressure-level-fields',
                    dest='pressure_level_params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=[],
                    help='one or more pressure-level fields that should be downloaded, e.g. u, v, p, m, ...')

parser.add_argument('--time-invariant-fields',
                    dest='time_invariant_params',
                    nargs='+',
                    metavar='shortName',
                    type=str,
                    default=[],
                    help='one or more time invariant fields that should be downloaded, e.g. hhl, ...')

parser.add_argument('--min-model-level',
                    dest='minModelLevel',
                    default=0,
                    type=int,
                    metavar='LEVEL',
                    help='the minimum level number to download (default=0)')

parser.add_argument('--max-model-level', dest='maxModelLevel', default=0,  # 90,
                    type=int,
                    metavar='LEVEL',
                    help='the maximum level number to download (default=0)')

parser.add_argument('--pressure-levels', dest='pressureLevels',
                    default=[],
                    type=str,
                    nargs='+',
                    metavar='PRESSURELEVEL',
                    help='a list of pressure levels. e.g. 1000 975 950 850')

parser.add_argument('--min-time-step',
                    dest='minTimeStep',
                    default=0,
                    type=int,
                    metavar='STEP',
                    help='the minimum forecast time step to download (default=0)')

parser.add_argument('--max-time-step',
                    dest='maxTimeStep',
                    default=0,
                    type=int,
                    metavar='STEP',
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

parser.add_argument("-f", "--flat",
                    help="store all files under --directory "
                    "-  default is to retain the opendata.dwd.de directory structure "
                    "and create subdirectories under  --directory as needed",
                    action="store_true",
                    dest="flat")

parser.add_argument("-c", "--compressed", help="store as bz2 file (do not uncompress)",
                    action="store_true", dest="compressed")
parser.add_argument("-r", "--reload", help="reload files even if bz2 file exists - default to skipping existing files",
                    action="store_false", default=True, dest="skipexisting")

parser.add_argument('--max-workers', dest='maxWorkers', default=20, type=int,
                    help='number of thread workers for parallel download')


"""
usage: opendata-downloader.py [-h] --model {cosmo-d2,cosmo-d2-eps,icon,icon-eps,icon-eu,icon-eu-eps,icon-d2,icon-d2-eps} [--grid {icosahedral,regular-lat-lon,rotated-lat-lon}]
                              [--get-latest-timestamp] [--single-level-fields shortName [shortName ...]] [--model-level-fields shortName [shortName ...]]
                              [--pressure-level-fields shortName [shortName ...]] [--time-invariant-fields shortName [shortName ...]] [--min-model-level LEVEL]
                              [--max-model-level LEVEL] [--pressure-levels PRESSURELEVEL [PRESSURELEVEL ...]] [--min-time-step STEP] [--max-time-step STEP]
                              [--directory DESTFILEPATH] [--modelrun MODELRUN] [--http-proxy proxy_name_or_ip:port] [-v] [-d] [-f] [-c] [-r] [--max-workers MAXWORKERS]

A tool to download grib model data from DWD's open data server https://opendata.dwd.de .

optional arguments:
  -h, --help            show this help message and exit
  --model {cosmo-d2,cosmo-d2-eps,icon,icon-eps,icon-eu,icon-eu-eps,icon-d2,icon-d2-eps}
                        the model name
  --grid {icosahedral,regular-lat-lon,rotated-lat-lon}
                        the grid type
  --get-latest-timestamp
                        Returns the latest available timestamp for the specified model.
  --single-level-fields shortName [shortName ...]
                        one or more single-level model fields that should be downloaded, e.g. t_2m, tmax_2m, clch, pmsl, ...
  --model-level-fields shortName [shortName ...]
                        one or more model-level fields that should be downloaded, e.g. u, v, p, m, ...
  --pressure-level-fields shortName [shortName ...]
                        one or more pressure-level fields that should be downloaded, e.g. u, v, p, m, ...
  --time-invariant-fields shortName [shortName ...]
                        one or more time invariant fields that should be downloaded, e.g. hhl, ...
  --min-model-level LEVEL
                        the minimum level number to download (default=0)
  --max-model-level LEVEL
                        the maximum level number to download (default=0)
  --pressure-levels PRESSURELEVEL [PRESSURELEVEL ...]
                        a list of pressure levels. e.g. 1000 975 950 850
  --min-time-step STEP  the minimum forecast time step to download (default=0)
  --max-time-step STEP  the maximung forecast time step to download, e.g. 12 will download time steps from min-time-step - 12. If no max-time-step was defined, no data will be
                        downloaded.
  --directory DESTFILEPATH
                        the download directory
  --modelrun MODELRUN   explicitly download from a particular model run. Example: --modelrun 2020121212
  --http-proxy proxy_name_or_ip:port
                        the http proxy url and port
  -v, --verbose         increase output verbosity
  -d, --dry-run         only show debug output, do not download
  -f, --flat            store all files in under --directory - default is to retain the opendata.dwd.de directory structure and create subdirectories under --directory as needed
  -c, --compressed      store as bz2 file (do not uncompress)
  -r, --reload          reload files even if bz2 file exists - default to skipping existing files
  --max-workers MAXWORKERS
                        number of thread workers for parallel download
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
    maxWorkers = args.maxWorkers

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
        log.info("Acquiring latest timestamp")
        print(getTimestampString(latestTimestamp))
        sys.exit(0)

    if args.single_level_params is None and args.model_level_params is None and args.time_invariant_params is None and args.pressure_level_params is None:

        log.error("nothing to download. Specify  any of: "
                  "--single-level-fields <fields>, "
                  "--model-level-fields <fields>, "
                  "--pressure-level-fields <fields> or "
                  "--time-invariant-fields <fields>")
        sys.exit(1)

    timeSteps = list(range(args.minTimeStep, args.maxTimeStep + 1))
    minModelLevel = args.minModelLevel if args.minModelLevel > 0 else selectedModel.get(
        "minlevel", 0)
    maxModelLevel = args.maxModelLevel if args.maxModelLevel > 0 else selectedModel.get(
        "maxlevel", 0)
    levelRange = list(range(minModelLevel, maxModelLevel + 1))

    for param in args.single_level_params:
        downloadGribDataSequence(model=selectedModel["model"],
                                 flat=args.flat,
                                 grid=args.grid,
                                 param=param,
                                 timeSteps=timeSteps,
                                 timestamp=latestTimestamp,
                                 levelRange=[0],
                                 levtype="single-level",
                                 destFilePath=args.destFilePath)

    for param in args.model_level_params:
        downloadGribDataSequence(model=selectedModel["model"],
                                 flat=args.flat,
                                 grid=args.grid,
                                 param=param,
                                 timeSteps=timeSteps,
                                 timestamp=latestTimestamp,
                                 levelRange=levelRange,
                                 levtype="model-level",
                                 destFilePath=args.destFilePath)

    for param in args.pressure_level_params:
        downloadGribDataSequence(model=selectedModel["model"],
                                 flat=args.flat,
                                 grid=args.grid,
                                 param=param,
                                 timeSteps=timeSteps,
                                 timestamp=latestTimestamp,
                                 levelRange=args.pressureLevels,
                                 levtype="pressure-level",
                                 destFilePath=args.destFilePath)

    for param in args.time_invariant_params:
        downloadGribDataSequence(model=selectedModel["model"],
                                 flat=args.flat,
                                 grid=args.grid,
                                 param=param,
                                 timeSteps=timeSteps,
                                 timestamp=latestTimestamp,
                                 levelRange=[0],
                                 levtype="time-invariant",
                                 destFilePath=args.destFilePath)

"""
if failedFiles:
    print(f"#the command line was: {sys.argv}", file=sys.stderr)
    print("#the below URLs failed to download:", file=sys.stderr)

for f in failedFiles:
    exc, url, status = f
    print(f"{exc} {url} {status}", file=sys.stderr)

sys.exit(len(failedFiles))
"""
