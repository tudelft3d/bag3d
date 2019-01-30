CREATE OR REPLACE VIEW bagactueel.missing_height AS
SELECT *
FROM bagactueel.bag3d
WHERE
(
"ground-0.00" IS NULL OR 
"ground-0.10" IS NULL OR 
"ground-0.20" IS NULL OR 
"ground-0.30" IS NULL OR 
"ground-0.40" IS NULL OR 
"ground-0.50" IS NULL
) AND ( 
"roof-0.00" IS NULL OR
"roof-0.10" IS NULL OR
"roof-0.25" IS NULL OR
"roof-0.50" IS NULL OR
"roof-0.75" IS NULL OR
"roof-0.90" IS NULL OR
"roof-0.95" IS NULL OR
"roof-0.99" IS NULL
);
COMMENT ON VIEW bagactueel.missing_height IS 'Buildings where any of the ground or roof heights is missing';

CREATE OR REPLACE VIEW bagactueel.bag3d_invalid_height AS
SELECT *
FROM bagactueel.bag3d
WHERE bouwjaar > date_part('YEAR', ahn_file_date);
COMMENT ON VIEW bagactueel.bag3d_invalid_height IS 'The BAG footprints where the building was built after the AHN3 was created';


CREATE OR REPLACE VIEW bagactueel.missing_ground AS
SELECT *
FROM bagactueel.bag3d
WHERE
"ground-0.00" IS NULL OR 
"ground-0.10" IS NULL OR 
"ground-0.20" IS NULL OR 
"ground-0.30" IS NULL OR 
"ground-0.40" IS NULL OR 
"ground-0.50" IS NULL;
COMMENT ON VIEW bagactueel.missing_ground IS 'Buildings where any of the ground heights is missing';


CREATE OR REPLACE VIEW bagactueel.missing_roof AS
SELECT *
FROM bagactueel.bag3d
WHERE
"roof-0.00" IS NULL OR
"roof-0.10" IS NULL OR
"roof-0.25" IS NULL OR
"roof-0.50" IS NULL OR
"roof-0.75" IS NULL OR
"roof-0.90" IS NULL OR
"roof-0.95" IS NULL OR
"roof-0.99" IS NULL;
COMMENT ON VIEW bagactueel.missing_roof IS 'Buildings where any of the roof heights is missing';


CREATE OR REPLACE VIEW bagactueel.bag3d_sample AS
WITH sample AS (
    SELECT *
    FROM bagactueel.bag3d
    TABLESAMPLE BERNOULLI (1)
)
SELECT
FROM sample s,
;

COMMENT ON VIEW bagactueel.bag3d_sample IS 'Random sample (1%) of the 3D BAG, using Bernoulli sampling method';


/* Evaluation */

WITH total AS (
    SELECT
        COUNT(gid) total_cnt
    FROM
        bagactueel.pand3d
),
ground AS (
    SELECT
        COUNT(gid) ground_missing_cnt
    FROM
        bagactueel.pand3d
    WHERE nr_ground_pts = 0
),
roof AS (
    SELECT
        COUNT(gid) roof_missing_cnt
    FROM
        bagactueel.pand3d
    WHERE nr_roof_pts = 0
),
invalid AS (
    SELECT
        COUNT (gid) invalid_height_cnt
    FROM
        bagactueel.pand3d
    WHERE bouwjaar > ahn_file_date
)
INSERT INTO public.bag3d_quality
SELECT
    current_date AS date,
    t.total_cnt,
    (t.total_cnt::float4 - i.invalid_height_cnt::float4) / t.total_cnt::float4 * 100 AS valid_height_pct,
    i.invalid_height_cnt::float4 / t.total_cnt::float4 * 100 AS invalid_height_pct,
    (
        g.ground_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
    )* 100 AS ground_missing_pct,
    (
        r.roof_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
    )* 100 AS roof_missing_pct
FROM
    total t,
    ground g,
    roof r,
    invalid i;

   
CREATE TABLE public.bag3d_quality (
date date PRIMARY KEY, 
total_cnt int,
valid_height_pct float4,
invalid_height_pct float4,
ground_missing_pct float4,
roof_missing_pct float4
);

-- Count the nr. of footprints per tile in the BAG and the 3D BAG
WITH bag_tiles AS (
SELECT
    pandactueelbestaand.gid,
    bag_index.bladnr
FROM
    bagactueel.pandactueelbestaand
JOIN bagactueel.pand_centroid ON
    pandactueelbestaand.gid = pand_centroid.gid,
    tile_index.bag_index
WHERE
    st_containsproperly(bag_index.geom,pand_centroid.geom)
    OR st_contains(bag_index.geom_border,pand_centroid.geom)
),
bag_tiles_cnt AS (
    SELECT bladnr AS tile_id, count(*) AS bag_cnt
    FROM bag_tiles
    GROUP BY bladnr
),
bag3d_tiles_cnt AS (
    SELECT tile_id, count(*) AS bag3d_cnt
    FROM bagactueel.pand3d
    GROUP BY tile_id
),
counts AS (
    SELECT a.tile_id, bag_cnt, bag3d_cnt
    FROM bag_tiles_cnt a
    LEFT JOIN bag3d_tiles_cnt b ON a.tile_id = b.tile_id
)
SELECT array_to_json(array_agg(counts)) AS building_cnt
FROM counts;


