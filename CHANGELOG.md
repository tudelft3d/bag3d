# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/).

## [Unreleased]
### Data
+ Removed the height values for the percentiles 00 and 10 (`roof-00, roof-10, rmse-00, rmse-10`), because these values are heavily skewed by points from the walls, thus they are don't indicate the building's height reliably.

+ Fixed the missing tiles issue.

### Software

