USE CATALOG retailflow_catalog;

-- 1. Define the dynamic masking function logic
CREATE OR REPLACE FUNCTION silver.email_mask_fn(email STRING)
RETURNS STRING
RETURN CASE 
  WHEN is_account_group_member('data_admins') THEN email
  ELSE regexp_replace(email, '(?<=.).(?=.*@)', '*')
END;

-- 2. Bind the function rule to the target silver column
ALTER TABLE silver.customers ALTER COLUMN email SET MASK silver.email_mask_fn;


SELECT customer_id, first_name, email FROM retailflow_catalog.silver.customers LIMIT 1;
-- Output shows unmasked data:
-- 3862 | Abhay | abhay.analytics@domain.com

SELECT customer_id, first_name, email FROM retailflow_catalog.silver.customers LIMIT 1;
-- Output shows obfuscated data:
-- 3862 | Abhay | a*****************s@domain.com

