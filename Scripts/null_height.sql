-- find footprints where the ground/and/or roof has null height

CREATE OR REPLACE VIEW bagactueel.no_ground AS
SELECT 
--gid, identificatie, geovlak
*
FROM bagactueel.bag3d
WHERE
(
"ground-0.00" IS NULL AND 
"ground-0.10" IS NULL AND 
"ground-0.20" IS NULL AND 
"ground-0.30" IS NULL AND 
"ground-0.40" IS NULL AND 
"ground-0.50" IS NULL
) AND ( 
"roof-0.00" IS NOT NULL OR
"roof-0.10" IS NOT NULL OR
"roof-0.25" IS NOT NULL OR
"roof-0.50" IS NOT NULL OR
"roof-0.75" IS NOT NULL OR
"roof-0.90" IS NOT NULL OR
"roof-0.95" IS NOT NULL OR
"roof-0.99" IS NOT NULL
);

CREATE OR REPLACE VIEW bagactueel.no_roof AS
SELECT 
--gid, identificatie, geovlak
*
FROM bagactueel.bag3d
WHERE
(
"roof-0.00" IS NULL AND
"roof-0.10" IS NULL AND
"roof-0.25" IS NULL AND
"roof-0.50" IS NULL AND
"roof-0.75" IS NULL AND
"roof-0.90" IS NULL AND
"roof-0.95" IS NULL AND
"roof-0.99" IS NULL
) AND (
"ground-0.00" IS NOT NULL OR
"ground-0.10" IS NOT NULL OR
"ground-0.20" IS NOT NULL OR
"ground-0.30" IS NOT NULL OR
"ground-0.40" IS NOT NULL OR
"ground-0.50" IS NOT NULL
);

CREATE OR REPLACE VIEW bagactueel.no_height AS
SELECT 
--gid, identificatie, geovlak
*
FROM bagactueel.bag3d
WHERE
"ground-0.00" IS NULL AND 
"ground-0.10" IS NULL AND 
"ground-0.20" IS NULL AND 
"ground-0.30" IS NULL AND 
"ground-0.40" IS NULL AND 
"ground-0.50" IS NULL AND
"roof-0.00" IS NULL AND
"roof-0.10" IS NULL AND
"roof-0.25" IS NULL AND
"roof-0.50" IS NULL AND
"roof-0.75" IS NULL AND
"roof-0.90" IS NULL AND
"roof-0.95" IS NULL AND
"roof-0.99" IS NULL;

CREATE OR REPLACE VIEW bagactueel.has_height AS
SELECT 
--gid, identificatie, geovlak
*
FROM bagactueel.bag3d
WHERE
"ground-0.00" IS NOT NULL AND 
"ground-0.10" IS NOT NULL AND 
"ground-0.20" IS NOT NULL AND 
"ground-0.30" IS NOT NULL AND 
"ground-0.40" IS NOT NULL AND 
"ground-0.50" IS NOT NULL AND
"roof-0.00" IS NOT NULL AND
"roof-0.10" IS NOT NULL AND
"roof-0.25" IS NOT NULL AND
"roof-0.50" IS NOT NULL AND
"roof-0.75" IS NOT NULL AND
"roof-0.90" IS NOT NULL AND
"roof-0.95" IS NOT NULL AND
"roof-0.99" IS NOT NULL;


SELECT * FROM bagactueel.no_ground LIMIT 50;
SELECT * FROM bagactueel.no_roof LIMIT 50;
SELECT * FROM bagactueel.no_height LIMIT 50;
SELECT * FROM bagactueel.has_height LIMIT 50;

