#! /bin/bash
docker run --name bag3d_postgis -v $(pwd):/tmp -w /tmp -p 5590:5432 -d mdillon/postgis:10
sleep 5
docker exec bag3d_postgis bash /tmp/init_db.sh
