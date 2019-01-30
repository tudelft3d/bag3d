#! /bin/bash
docker run --rm --name 3dbag_postgis -p 5590:5432 -d postgres:bag3d
