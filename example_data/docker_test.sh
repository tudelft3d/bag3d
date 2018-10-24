#! /bin/bash
# Test if the docker images work as expected

# First need to run ./postgis/docker_run_postgis.sh

docker run --rm --network=host -v $(pwd):/home/3dfier 3dbag:3dfier 3dfier cfg_test.yml --OBJ test.obj --CSV-BUILDINGS test.csv;

# Then ./postgis/docker_stop_postgis.sh