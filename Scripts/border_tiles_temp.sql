DROP TABLE IF EXISTS tile_index.border_tiles CASCADE;

CREATE TABLE tile_index.border_tiles AS
WITH ahn2 AS (
SELECT *
FROM tile_index.ahn_index
WHERE ahn_version = 2
),
ahn3 AS (
SELECT *
FROM tile_index.ahn_index
WHERE ahn_version = 3
)
SELECT DISTINCT ahn3.*
FROM ahn3, ahn2
WHERE st_touches(ahn3.geom, ahn2.geom);

COMMENT ON TABLE tile_index.border_tiles IS 'AHN tiles on the boarder of AHN3-AHN2. With AHN2 file creation date.';

UPDATE tile_index.border_tiles
SET ahn_version = 2;


SELECT
    bladnr
FROM
    tile_index.border_tiles;


-- select non-border tiles
SELECT
    a.a_bladnr AS bladnr
FROM
    (
        SELECT
            a.bladnr a_bladnr,
            b.bladnr b_bladnr
        FROM
            tile_index.ahn_index a
        LEFT JOIN tile_index.border_tiles b ON
            a.bladnr = b.bladnr
    ) a
WHERE
    a.b_bladnr IS NULL;


-- the united border tiles with ahn2 and ahn3
CREATE OR REPLACE VIEW bagactueel.bag3d_border_union AS
WITH border_ahn3_notnull AS(
    SELECT
        a.*
    FROM
        bagactueel.bag3d_border_ahn3 a
    WHERE
        a."ground-0.00" IS NOT NULL
        AND a."ground-0.10" IS NOT NULL
        AND a."ground-0.20" IS NOT NULL
        AND a."ground-0.30" IS NOT NULL
        AND a."ground-0.40" IS NOT NULL
        AND a."ground-0.50" IS NOT NULL
        AND a."roof-0.00" IS NOT NULL
        AND a."roof-0.10" IS NOT NULL
        AND a."roof-0.25" IS NOT NULL
        AND a."roof-0.50" IS NOT NULL
        AND a."roof-0.75" IS NOT NULL
        AND a."roof-0.90" IS NOT NULL
        AND a."roof-0.95" IS NOT NULL
        AND a."roof-0.99" IS NOT NULL
),
border_ahn2_id AS(
    SELECT
        ARRAY_AGG( a.identificatie ) identificatie
    FROM
        (
            SELECT
                identificatie
            FROM
                bagactueel.bag3d_border_ahn2
        EXCEPT SELECT
                identificatie
            FROM
                border_ahn3_notnull
        ) a
) SELECT
    *
FROM
    border_ahn3_notnull
UNION SELECT
    a.*
FROM
    bagactueel.bag3d_border_ahn2 a,
    border_ahn2_id b
WHERE
    a.identificatie = ANY(b.identificatie);
   
-- sanity check
SELECT count(*) FROM bagactueel.bag3d_border_ahn2;
SELECT count(*) FROM bagactueel.bag3d_border_ahn3;
SELECT count(*) FROM bagactueel.bag3d_border_union;


-- the final 3D BAG table with sanitized border tiles
CREATE TABLE bagactueel.bag3d AS
SELECT *
FROM bagactueel.bag3d_rest
UNION
SELECT *
FROM bagactueel.bag3d_border_union;

CREATE INDEX bag3d_geom_idx ON bagactueel.bag3d USING gist (geovlak);
ALTER TABLE bagactueel.bag3d ADD PRIMARY KEY (gid);
COMMENT ON TABLE bagactueel.bag3d IS 'The 3D BAG';

