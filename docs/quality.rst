3D BAG quality expectations
###########################

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

3D BAG quality testing
######################

+ randomly sample 1-5% of buildings, take the AHN raster, compute the height percentiles and compare those to the 3D BAG

Because I will remove the tile-tile intersection so that when complete tiles
are provided as input extent, the 9 neighbouring tiles won't get dragged in 
the computation:

+ randomly sample 1-5% buildings that intersect the borders of the tile polygons and perform the same percentile comparison as above
