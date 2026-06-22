-- Bulk-load the static/reference tables via server-side LOAD DATA INFILE.
-- CSVs are bind-mounted read-only at /csv, which secure_file_priv points to.
-- The order lifecycle tables are intentionally left empty.

LOAD DATA INFILE '/csv/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state);

LOAD DATA INFILE '/csv/olist_sellers_dataset.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(seller_id, seller_zip_code_prefix, seller_city, seller_state);

-- Numeric columns are blank for a few products; map via @vars and NULLIF.
LOAD DATA INFILE '/csv/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(product_id, @cat, @nl, @dl, @pq, @wg, @lcm, @hcm, @wcm)
SET product_category_name      = NULLIF(@cat, ''),
    product_name_length        = NULLIF(@nl, ''),
    product_description_length = NULLIF(@dl, ''),
    product_photos_qty         = NULLIF(@pq, ''),
    product_weight_g           = NULLIF(@wg, ''),
    product_length_cm          = NULLIF(@lcm, ''),
    product_height_cm          = NULLIF(@hcm, ''),
    product_width_cm           = NULLIF(@wcm, '');

-- This file has a UTF-8 BOM (skipped by IGNORE 1 LINES) and CRLF line endings,
-- so strip the trailing CR left on the last column.
LOAD DATA INFILE '/csv/product_category_name_translation.csv'
INTO TABLE product_category_name_translation
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(product_category_name, @english)
SET product_category_name_english = TRIM(TRAILING '\r' FROM @english);

LOAD DATA INFILE '/csv/olist_geolocation_dataset.csv'
INTO TABLE geolocation
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state);
