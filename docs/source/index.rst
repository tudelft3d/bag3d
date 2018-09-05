.. bag3d documentation master file, created by
   sphinx-quickstart on Thu May 17 17:10:10 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

3D BAG
======

This is the documentation of the **3D BAG** data set, as well as the ``bag3d`` software that is used for generating the data. The 3D BAG is an enhanced version of the Dutch `Basisregistraties Adressen en Gebouwen (BAG) <https://www.kadaster.nl/wat-is-de-bag>`_ data set, with added height information. The 3D BAG lives in a PostgreSQL database and it follows the updates of the BAG.

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   
   quality
   software



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

The `Basisregistraties Adressen en Gebouwen (BAG) <https://www.kadaster.nl/wat-is-de-bag>`_ is the most detailed, openly available data set on buildings in the Netherlands. It contains information about each address in a building, such as its current use, construction date or registration status. The polygons in the BAG represent the outline of the corresponding building's roof. The data set is regularly updated as new buildings are registered, built or demolished. The project `NLExtract <http://www.nlextract.nl/>`_ prepares a monthly PostgreSQL backup of the BAG, which is then used as basis for the 3D BAG.

The `Actueel Hoogtebestand Nederland (AHN) <http://www.ahn.nl>`_ is the openly available elevation model of the Netherlands obtained by aerial laser scanning. It is accessible in raster and raw point cloud (LAZ) format. The current is the third version which does not cover the whole country yet, therefore it needs to be extended with the previous version to obtain full coverage. The for disseminating the AHN efficiently, the Netherlands split into 1377 tiles.

The engine behind ``bag3d`` is `3dfier <https://github.com/tudelft3d/3dfier>`_ which takes 2D GIS data sets (e.g BAG) and *3dfies* them by lifting each polygon to a height obtained from a point cloud (e.g. AHN), thus generating LoD1 models (block models). Although for LoD1 building models the top surface (or roof) is set to a uniform height per building, one of the strengths of 3dfier is the possibility to set the height of the top surface at the desired height relative to the points representing the building.


