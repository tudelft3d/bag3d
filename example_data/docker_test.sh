#! /bin/bash
# Test if the docker images work as expected

# First need to run ./postgis/docker_run_postgis.sh

docker run --rm --network=host -v $(pwd):/home/3dfier 3dfier:bag3d 3dfier /home/3dfier/3dfier_test.yml --OBJ /home/3dfier/test.obj --CSV-BUILDINGS /home/3dfier/test.csv;

# Then ./postgis/docker_stop_postgis.sh
