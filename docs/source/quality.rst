Quality of the 3D BAG
######################

Quality expectations
============================

These quality expectations describe the 3D-part of the 3D BAG. The quality of the BAG itself is not discussed here.

Completeness
*************

* Every BAG building footprint polygon should have a height assigned to it wherever AHN has data.

* The date of AHN (height information) is assigned to every footprint. This includes the AHN version.

* The binary attribute `flat_roof` is assigned to every footprint with height information.

Correctness
***********

* Which part of the roof does `roof-0.00`, `roof-0.50` and `roof-0.99` relate to in the real building?

* The newest avialable AHN is used for every footprint.

Consistency
************

Coherence
*********

Accountability
***************

* The source of BAG and AHN is clearly stated.

* Logging into the `bagactueel.bag3d_log` table.

* Data quality testing summary into the `bagactueel.bag3d_info` table.

Quality testing
=======================

+ randomly sample 1-5% of buildings, take the AHN raster, compute the height percentiles and compare those to the 3D BAG

Because I will remove the tile-tile intersection so that when complete tiles
are provided as input extent, the 9 neighbouring tiles won't get dragged in 
the computation:

+ randomly sample 1-5% buildings that intersect the borders of the tile polygons and perform the same percentile comparison as above


Comparison with the AHN 0.5m raster
***********************************
So, use rasterstats library to compute zonal statistics from the AHN raster. In this case a zone is a single building footprint and the statistics are all the roof-height percentiles.

0. Download the AHN3 0.5m raster and the AHN2 0.5m raster (only for those tiles where AHN3 is not available)

wget -nc https://geodata.nationaalgeoregister.nl/ahn3/extract/ahn3_05m_dsm/R_37FZ1.ZIP -O tile && unzip -o tile && rm tile
wget -nc http://geodata.nationaalgeoregister.nl/ahn2/extract/ahn2_05m_ruw/r37hz1.tif.zip -O tile && unzip -o tile && rm tile

1. Randomly sample 1% of the 3D BAG
2. Get the geometry from postgres into python as WKB (or WKT?)
3. Also get the tile ID for each geometry
4. Create an index of tile IDs and raster files

src = [wkb1, wkb2, wkb3, ...]
zs = zonal_stats(src, 'tests/data/slope.tif')

