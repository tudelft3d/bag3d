# -*- coding: utf-8 -*-

"""Module description"""

from math import sqrt

from psycopg2 import sql
from psycopg2 import extras
import numpy as np

from bag3d.config import db
from rasterstats import zonal_stats


conn = db.db(dbname="bag3d_test", host="localhost",
             port=5555, user="bdukai")

query = sql.SQL("""
SELECT 
ST_AsEWKB({geom}) geom,
"roof-0.00" "percentile_0.00",
"roof-0.10" "percentile_0.10",
"roof-0.25" "percentile_0.25",
"roof-0.50" "percentile_0.50",
"roof-0.75" "percentile_0.75",
"roof-0.90" "percentile_0.90",
"roof-0.95" "percentile_0.95",
"roof-0.99" "percentile_0.99"
FROM bagactueel.{bag3d} 
TABLESAMPLE BERNOULLI (1) 
WHERE {tile_id} = {tile};
""").format(geom=sql.Identifier("geovlak"),
            bag3d=sql.Identifier("pand3d"),
            tile_id=sql.Identifier("tile_id"),
            tile=sql.Literal("37hz1"))

print(conn.print_query(query))
res = conn.get_dict(query)
polys = [bytes(row['geom']) for row in res]

stats=['percentile_0.00', 'percentile_0.10', 'percentile_0.25',
       'percentile_0.50', 'percentile_0.75', 'percentile_0.90',
       'percentile_0.95', 'percentile_0.99']
ref_heights = zonal_stats(polys, '/home/balazs/Data/bag3d_test/raster/r37hz1.tif',
                 stats=stats)

def compute_diffs(stats, heights, ref_heights):
    """
    
    Parameters
    ----------
    stats : list of str
        Dictionary keys
    heights : dict
    ref_heights : dict
    
    Returns
    -------
    dict
        {percentile : Numpy Array of 'reference-height - computed-height' differences}
    """
    diffs = {}
    for k in stats:
        diffs[k] = []
        for i, row in enumerate(heights):
            if ref_heights[i][k]:
                diffs[k].append(row[k]- ref_heights[i][k])
            else:
                diffs[k].append(None)
        diffs[k] = np.array(diffs[k], dtype='float32')
    return diffs

diffs = compute_diffs(stats, heights=res, ref_heights=ref_heights)

def rmse(a):
    """Compute Root Mean Square Error from a Numpy Array of height - reference 
    differences
    """
    return sqrt(sum([d**2 for d in a]) / len(a))

def compute_rmse(diffs):
    res = {}
    for k in diffs:
        res[k] = rmse(diffs[k])
    return res

errs = compute_rmse(diffs)


