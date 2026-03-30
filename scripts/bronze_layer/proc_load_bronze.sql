CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_seconds NUMERIC;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	batch_start_time:= clock_timestamp()
    RAISE NOTICE '==============================';
    RAISE NOTICE 'Loading bronze layer started';
    RAISE NOTICE '==============================';

    -- crm_cust_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE '>> Loading table: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/import/source_crm/cust_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load duration for bronze.crm_cust_info: % seconds', duration_seconds;

    -- crm_prd_info
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE '>> Loading table: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/import/source_crm/prd_info.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load duration for bronze.crm_prd_info: % seconds', duration_seconds;

    -- crm_sales_details
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE '>> Loading table: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/import/source_crm/sales_details.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load duration for bronze.crm_sales_details: % seconds', duration_seconds;

    -- erp_loc_a101
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating table: bronze.erp_loc_a101';
    TRUNCATE TABLE bronze.erp_loc_a101;

    RAISE NOTICE '>> Loading table: bronze.erp_loc_a101';
    COPY bronze.erp_loc_a101
    FROM '/import/source_erp/LOC_A101.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load duration for bronze.erp_loc_a101: % seconds', duration_seconds;

    -- erp_cust_az12
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating table: bronze.erp_cust_az12';
    TRUNCATE TABLE bronze.erp_cust_az12;

    RAISE NOTICE '>> Loading table: bronze.erp_cust_az12';
    COPY bronze.erp_cust_az12
    FROM '/import/source_erp/CUST_AZ12.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load duration for bronze.erp_cust_az12: % seconds', duration_seconds;

    -- erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE NOTICE '>> Truncating table: bronze.erp_px_cat_g1v2';
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    RAISE NOTICE '>> Loading table: bronze.erp_px_cat_g1v2';
    COPY bronze.erp_px_cat_g1v2
    FROM '/import/source_erp/PX_CAT_G1V2.csv'
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE NOTICE '>> Load duration for bronze.erp_px_cat_g1v2: % seconds', duration_seconds;

    RAISE NOTICE '==============================';
    RAISE NOTICE 'Bronze layer load finished';
    RAISE NOTICE '==============================';
	batch_end_time:= clock_timestamp()
	duration_seconds := EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
END;
$$;
