Download the data
##################

**WFS:** ``http://godzilla.bk.tudelft.nl:8090/geoserver/BAG3D/wfs``

**CSV:** `<http://godzilla.bk.tudelft.nl/data/3dbag/csv/>`_

The Comma Separated Value file is an export of the 3D BAG attributes only, thus does not contain the geometry of the building footprints.

**GPKG:** `<http://godzilla.bk.tudelft.nl/data/3dbag/gpkg/>`_

The `GeoPackage <http://www.geopackage.org/>`_ is an export of the complete 3D BAG (incl. geometry).

**PostgreSQL backup:** `<http://godzilla.bk.tudelft.nl/data/3dbag/postgis/>`_

The PostgreSQL backup file is equivalent in content to the GPKG. For example the backup can be restored as:

.. code-block:: sh

    createdb <db>
    psql -d <db> -c 'create extension postgis;'
    pg_restore --no-owner --no-privileges -h <host> -U <user> -d <db> -w bagactueel_schema.backup
    pg_restore --no-owner --no-privileges -j 2 --clean -h <host> -U <user> -d <db> -w <bag3d backup>.backup
