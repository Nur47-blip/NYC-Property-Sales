-- Membuat database
CREATE DATABASE m3

-- Membuat tabel
CREATE TABLE table_m3 (
    row_index INTEGER,
    BOROUGH FLOAT,
    NEIGHBORHOOD TEXT,
    "BUILDING CLASS CATEGORY" TEXT,
    "TAX CLASS AT PRESENT" TEXT,
    BLOCK FLOAT,
    LOT FLOAT,
    "EASE-MENT" TEXT,
    "BUILDING CLASS AT PRESENT" TEXT,
    ADDRESS TEXT,
    "APARTMENT NUMBER" TEXT,
    "ZIP CODE" FLOAT,
    "RESIDENTIAL UNITS" FLOAT,
    "COMMERCIAL UNITS" FLOAT,
    "TOTAL UNITS" FLOAT,
    "LAND SQUARE FEET" TEXT,
    "GROSS SQUARE FEET" TEXT,
    "YEAR BUILT" FLOAT,
    "TAX CLASS AT TIME OF SALE" FLOAT,
    "BUILDING CLASS AT TIME OF SALE" TEXT,
    "SALE PRICE" BIGINT,
    "SALE DATE" TEXT
);


-- Menggunakan PSQL untuk memasukkan data ke postgreSQL
psql -h localhost -U postgres -d m3

\copy table_m3(row_index, BOROUGH, NEIGHBORHOOD, "BUILDING CLASS CATEGORY", "TAX CLASS AT PRESENT", BLOCK, LOT, "EASE-MENT", "BUILDING CLASS AT PRESENT", ADDRESS, "APARTMENT NUMBER", "ZIP CODE", "RESIDENTIAL UNITS", "COMMERCIAL UNITS", "TOTAL UNITS", "LAND SQUARE FEET", "GROSS SQUARE FEET", "YEAR BUILT", "TAX CLASS AT TIME OF SALE", "BUILDING CLASS AT TIME OF SALE", "SALE PRICE", "SALE DATE") FROM 'C:\Users\LENOVO\github-classroom\FTDS-assignment-bay\p2-ftds023-rmt-m3-Nur47-blip\P2M3_nur_alamsyah_data_raw.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
