# -*- coding: utf-8 -*-

"""Update the AHN files and tile index"""

import os.path
import subprocess
import locale
from datetime import datetime

import re
import urllib.request, json
import logging

logger = logging.getLogger('update.ahn')

def update_json_id(json):
    """Update a GeoJSON tile index's ID field
    
    Overwrites or adds the "id" field as sequential integer
    
    Parameters
    ----------
    json: a parsed GeoJSON
    
    Returns
    -------
    The modified GeoJSON
    """
    for i,f in enumerate(json["features"]):
        json["features"][i]["id"] = i+1
        json["features"][i]["properties"]["id"] = i+1
    return json


def download_ahn_index():
    """Download the newest AHN3 units/index file"""

    with urllib.request.urlopen("https://geodata.nationaalgeoregister.nl/ahn3/wfs?SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature&outputFormat=application/json&TYPENAME=ahn3:ahn3_bladindex&SRSNAME=EPSG:28992") as url:
        data = json.loads(url.read().decode())
        return data


def get_file_date(ahn_dir, ahn_pat, t, f_date_pat, corruptedfiles):
    """Get the file creation date from a las file"""
    try:
        p = os.path.join(ahn_dir, ahn_pat.format(t))
    except KeyError:
        p = os.path.join(ahn_dir, ahn_pat.format(tile=t))
    except KeyError as e:
        logger.error("Cannot format %s", ahn_pat)
        logger.error(e)
        
    # lasinfo is not compiled with multi-core support on godzilla
    check = subprocess.run(['lasinfo', p, '-nc'], 
                           stdout=subprocess.PIPE, 
                           stderr=subprocess.PIPE)
    # by default LAStools outputs everything to stderr instead of stdout
    err = check.stderr.decode(locale.getpreferredencoding(do_setlocale=True))
    if "ERROR" in err:
        logger.error("Tile with error: %s", t)
        logger.error(err)
        corruptedfiles.append(t)
        d = None
    else:
        # append file creation date to AHN index
        f_date = f_date_pat.search(err).group().strip()
        day_len = f_date.find("/")
        if day_len == 1:
            date = "00" + f_date
        elif day_len == 2:
            date = "0" + f_date
        else:
            date = f_date
        # strftime needs zero-padding for day-of-the-year
        d = datetime.strptime(date + '/CET', '%j/%Y/%Z')
        
    return d


def download(ahn3_dir, ahn2_dir, tile_index_file, ahn3_file_pat, ahn2_file_pat):
    """Update the AHN3 files in the provided folder

    1. Downloads the latest AHN3 index (bladindex) to the local file system
    2. Downloads all AHN3 tiles that are not in the provided directory and checks 
    them for error with lasinfo (without parsing the points).
    3. Appends the 'file creation date' attribute of the LAZ file to the AHN index.
    4. If an AHN3 file is not available, marks the tile as AHN2 and add the date of the AHN2 file
    
    Parameters
    ----------
    ahn3_dir: path to the directory for the AHN3 files
    ahn2_dir: path to the directory for the AHN2 files
    tile_index_file: path for the AHN tile index
    """
    logger.debug("download() %s", (ahn3_dir, ahn2_dir, tile_index_file, ahn3_file_pat, ahn2_file_pat))
    
    f_idx = os.path.abspath(tile_index_file)

    # Get AHN3 index
    j_in = download_ahn_index()
    has_data_cnt = 0
    ahn_idx = {i: tile['properties']['bladnr'] for i, tile in enumerate(j_in['features'])}
    # how many AHN3 tiles are available
    for i, tile in enumerate(j_in['features']):
        if tile['properties']['has_data']:
            has_data_cnt += 1
    
    # Parse download URLs
    ahn_pat = ahn3_file_pat
    ahn2_pat = ahn2_file_pat # in /data/pointcloud/AHN2/uitgefiltered
    f_date_pat = re.compile(r"(?<=\sfile creation day/year:).*", 
                            flags=re.IGNORECASE & re.MULTILINE)
    url = "https://geodata.nationaalgeoregister.nl/ahn3/extract/ahn3_laz/C_{}.LAZ"
    
    downloaded = 0
    ahn2_files = 0
    corruptedfiles = []

    for i, tile in ahn_idx.items():
        logger.debug("Downloading file # %s out of %s", str(i), str(len(ahn_idx)))
        t = tile.upper()
        command = ['wget', '-nc', '-P', ahn3_dir, url.format(t)]
        logger.debug(command)
        dl = subprocess.run(command,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
        err = dl.stderr.decode(locale.getpreferredencoding(do_setlocale=True))
        if "already there" in err:
            add_date = True
        elif dl.returncode != 0:
            add_date = False
        elif dl.returncode == 0:
            downloaded += 1
            add_date = True

        if add_date:
            d = get_file_date(ahn3_dir, ahn_pat, t, f_date_pat, corruptedfiles)
            if d:
                j_in['features'][i]['properties']['file_date'] = d.isoformat()
                j_in['features'][i]['properties']['ahn_version'] = 3
        elif add_date is False and j_in['features'][i]['properties']['has_data'] is True:
            logger.info("Tile %s is not available, but marked as such. Correcting tile index...", t)
            j_in['features'][i]['properties']['has_data'] = False
            j_in['features'][i]['properties']['file_date'] = None
            j_in['features'][i]['properties']['ahn_version'] = 2
            
        else:
            logger.info("AHN2 tile: %s", t)
            ahn2_files += 1
            t = tile.lower()
            d = get_file_date(ahn2_dir, ahn2_pat, t, f_date_pat, corruptedfiles)
            if d:
                j_in['features'][i]['properties']['file_date'] = d.isoformat()
                j_in['features'][i]['properties']['ahn_version'] = 2

    cmd = " ".join(["ls -l", ahn3_dir, "| wc -l"])
    dl = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    out = int(dl.stdout.decode(locale.getpreferredencoding(do_setlocale=True)))
    file_count = out-1 # because the log of this script is also stored in the dir
    
    # set serial integer ID field
    j = update_json_id(j_in)
    
    with open(f_idx, 'w', encoding='utf-8') as f_out:
        json.dump(j, f_out)
    
    logger.info("Downloaded %s files", downloaded)
    logger.info("%s files are corrupted", len(corruptedfiles))
    logger.info("Corrupted files: %s", corruptedfiles)
    logger.info("Nr. AHN3 files in dir: %s; Nr. AHN3 tiles available: %s", file_count, has_data_cnt)
    logger.info("Nr. AHN2 files required: %s", ahn2_files)