-- Expand the JSON of the building counts to see which tiles are missing data from the 3D BAG
with expanded as (
  select tile_id, bag_cnt, bag3d_cnt
  from public.bag3d_quality as a,
       json_to_recordset(
           a.building_cnt
         ) as x(tile_id text, bag_cnt int, bag3d_cnt int)
)
select *
from expanded
where bag_cnt > bag3d_cnt;


select count(a.gid) as "3dbag_cnt", count(b.gid) as bag_cnt from "3dbag".pand3d a, bagactueel.pand b;