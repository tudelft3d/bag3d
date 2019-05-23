# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [1.0.2] - 2019-05-23
### Fixes
+ Paging in Geoserver by adding PKEY to production table. Issue [#26](https://github.com/tudelft3d/bag3d/issues/26)

+ Upgrade pyyaml. Issue [#25](https://github.com/tudelft3d/bag3d/issues/25)


## [1.0.0] - 2019-02-25
### Data
+ Removed the height values for the percentiles 00 and 10 (`roof-00, roof-10, rmse-00, rmse-10`), because these values are heavily skewed by points from the walls, thus they are don't indicate the building's height reliably.

+ Fixed the missing tiles issue.

### Software

+ Minimize downtime at update

+ Add staging area and data migration

+ Add the counts of the BAG and 3D BAG footprints per tile into the quality table

+ Switch to psutil to run subprocesses

+ Add --log agrument to set loglevel in the CLI

+ Restart failed processes

[Unreleased]: https://github.com/tudelft3d/bag3d/tree/develop
[1.0.0]: https://github.com/tudelft3d/bag3d/releases/tag/v1.0.0
[1.0.2]: https://github.com/tudelft3d/bag3d/releases/tag/v1.0.2
