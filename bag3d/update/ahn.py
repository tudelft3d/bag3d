# -*- coding: utf-8 -*-

"""Update the AHN files and tile index"""

import os
import os.path
import subprocess
import locale
from datetime import datetime
from shutil import which

import re
import urllib.request, json
import logging

from bag3d.config import border
from bag3d.update import bag

logger = logging.getLogger(__name__)

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
    for i,f in enumerate(json['features']):
        json['features'][i]['id'] = i+1
        json['features'][i]['properties']['id'] = i+1
    return json


def download_ahn_index():
    """Download the newest AHN3 units/index file"""

    with urllib.request.urlopen("https://geodata.nationaalgeoregister.nl/ahn3/wfs?SERVICE=WFS&VERSION=1.0.0&REQUEST=GetFeature&outputFormat=application/json&TYPENAME=ahn3:ahn3_bladindex&SRSNAME=EPSG:28992") as url:
        data = json.loads(url.read().decode())
        return data


def get_file_date(path_lasinfo, ahn_dir, ahn_pat, t, f_date_pat, corruptedfiles):
    """Get the file creation date from a las file"""
    try:
        p = os.path.join(ahn_dir, ahn_pat.format(t))
    except KeyError:
        p = os.path.join(ahn_dir, ahn_pat.format(tile=t))
    except KeyError as e:
        logger.error("Cannot format %s", ahn_pat)
        logger.error(e)
        
    # lasinfo is not compiled with multi-core support on godzilla
    check = subprocess.run([path_lasinfo, p, '-nc'], 
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
        sres = f_date_pat.search(err)
        logger.debug(sres)
        if sres:
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
        else:
            
            logger.error("Could not find the file date in LAS header:\n %s", err)
            return None


def download(path_lasinfo, ahn3_dir, ahn2_dir, tile_index_file, ahn3_file_pat, ahn2_file_pat):
    """Update the AHN3 files in the provided folder

    1. Downloads the latest AHN3 index (bladindex) to the local file system
    2. Downloads all AHN3 tiles that are not in the provided directory and checks them for error with lasinfo (without parsing the points).
    3. Appends the 'file creation date' attribute of the LAZ file to the AHN index.
    4. If an AHN3 file is not available, marks the tile as AHN2 and add the date of the AHN2 file.

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
            d = get_file_date(path_lasinfo, ahn3_dir, ahn_pat, t, f_date_pat, corruptedfiles)
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
            d = get_file_date(path_lasinfo, ahn2_dir, ahn2_pat, t, f_date_pat, corruptedfiles)
            if d:
                j_in['features'][i]['properties']['file_date'] = d.isoformat()
                j_in['features'][i]['properties']['ahn_version'] = 2

    cmd = " ".join(['ls -l", ahn3_dir, "| wc -l'])
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


def downloader(tile_list, url, dir_out, doexec=True):
    is_wget = which("wget")
    is_unzip = which("unzip")
    is_rm = which("rm")
    if is_wget is None:
        logger.error("'wget' not found, exiting")
        exit(1)
    if is_unzip is None:
        logger.error("'unzip' not found, exiting")
        exit(1)
    if is_rm is None:
        logger.error("'rm' not found, exiting")
        exit(1)
    try:
        fout = os.path.join(dir_out, 'tile')
        for tile in tile_list:
            command = ['wget', '-nc', '-P', dir_out, url.format(tile), '-O', fout]
            bag.run_subprocess(command, shell=False, doexec=doexec)
            command = ['unzip', '-o', fout, '-d', dir_out]
            bag.run_subprocess(command, shell=False, doexec=doexec)
            command = ['rm', fout]
            bag.run_subprocess(command, shell=False, doexec=doexec)
        logger.info("Downloaded %s tiles", len(tile_list))
    except Exception as e:
        logger.exception(e)


def download_raster(conn, config, ahn2_rast_dir, ahn3_rast_dir, doexec=True):
    "wget https://geodata.nationaalgeoregister.nl/ahn3/extract/ahn3_05m_dsm/R_37HN1.ZIP -O tile && unzip -o tile && rm tile"
    
    tbl_schema = config['tile_index']['elevation']['schema']
    tbl_name = config['tile_index']['elevation']['table']
    tbl_tile = config['tile_index']['elevation']['fields']['unit_name']
    border_table = config['tile_index']['elevation']['border_table']
    
    ahn2_url = "http://geodata.nationaalgeoregister.nl/ahn2/extract/ahn2_05m_ruw/r{}.tif.zip"
    ahn3_url = "https://geodata.nationaalgeoregister.nl/ahn3/extract/ahn3_05m_dsm/R_{}.ZIP"
    
    assert os.path.isdir(ahn2_rast_dir)
    assert os.path.isdir(ahn3_rast_dir)
    
    bt = border.get_non_border_tiles(conn, tbl_schema, tbl_name, 
                                     border_table, tbl_tile)
    ahn2_tiles = [t[0].lower() for t in bt if t[1] is not None and t[1]==2]
    ahn3_tiles = [t[0].upper() for t in bt if t[1] is not None and t[1]==3]
    
    downloader(ahn2_tiles, ahn2_url, ahn2_rast_dir, doexec)
    downloader(ahn3_tiles, ahn3_url, ahn3_rast_dir, doexec)


def rast_file_idx(conn, config, ahn2_rast_dir, ahn3_rast_dir):
    """Create an index of tiles and AHN raster files
    
    Parameters
    ----------
    ahn2_rast_dir : str
        Path to the directory of the AHN2 raster files
    ahn3_rast_dir : str
        Path to the directory of the AHN3 raster files
    
    Returns
    -------
    dict
        {tile ID : path to raster file}
    """
    assert os.path.isdir(ahn2_rast_dir)
    assert os.path.isdir(ahn3_rast_dir)
    ahn2_files = os.listdir(ahn2_rast_dir)
    ahn3_files = os.listdir(ahn3_rast_dir)
    
    tbl_schema = config['tile_index']['elevation']['schema']
    tbl_name = config['tile_index']['elevation']['table']
    tbl_tile = config['tile_index']['elevation']['fields']['unit_name']
    border_table = config['tile_index']['elevation']['border_table']
    bt = border.get_non_border_tiles(conn, tbl_schema, tbl_name, 
                                     border_table, tbl_tile)
    ahn2_tiles = [t[0].lower() for t in bt if t[1] is not None and t[1]==2]
    ahn3_tiles = [t[0].upper() for t in bt if t[1] is not None and t[1]==3]
    
    file_idx = {}
    for tile in ahn2_tiles:
        t = tile.lower()
        f = "r{}.tif".format(t)
        if f in ahn2_files:
            file_idx[t] = os.path.join(ahn2_rast_dir,f)
        else:
            pass
    for tile in ahn3_tiles:
        t = tile.lower()
        f = "r_{}.tif".format(t)
        if f in ahn3_files:
            file_idx[t] = os.path.join(ahn3_rast_dir,f)
        else:
            pass
    logger.debug(file_idx)
    return file_idx
