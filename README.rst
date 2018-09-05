******
3D BAG
******

|Licence| |Python 3.6| |PostgreSQL 10|

``bag3d`` is an application for generating a 3D version of the Dutch `Basisregistraties Adressen en Gebouwen (BAG) <https://www.kadaster.nl/wat-is-de-bag>`_ data set, by using `3dfier <https://github.com/tudelft3d/3dfier>`_ for extruding the building footprints to LoD1 models. It is designed to regularly run as an automated process (eg. with a cron job), hence keeping the 3D BAG in line with the BAG updates. The project started as `batch3dfier <https://github.com/balazsdukai/batch3dfier>`_ but it made sense to specialise it as more and more dataset specific features were needed.

In short the ``bag3d`` application can:

+ Download and restore a BAG PostgreSQL backup from `NLExtract <http://www.nlextract.nl/>`_.
+ Download all the available AHN3 files and the tile index. When new tiles are available, download only those that are not stored locally. Additionally, append the AHN file creation date to each tile in the index.
+ Import the tile indexes for BAG and AHN to a PostgreSQL database and partition the BAG building footprints.
+ Run several `3dfier <https://github.com/tudelft3d/3dfier>`_ jobs in parallel to process the given tiles. Currently the 3dfication output is attached to the 2D polygons as height attributes and the results are stored in the database.
+ Export into CSV, GeoPackage or PostgreSQL backup.
+ Report various quality metrics.

Read more about the 3D BAG data set as well as ``bag3d`` in the *documentation (LINK)*


.. |Licence| image:: https://img.shields.io/badge/licence-GPL--3-blue.svg
   :target: http://www.gnu.org/licenses/gpl-3.0.html
.. |Python 3.6| image:: https://img.shields.io/badge/python-3.6-blue.svg
.. |PostgreSQL 10| image:: https://img.shields.io/badge/PostgreSQL-10-blue.svg
