CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration_seconds NUMERIC;
BEGIN

    RAISE NOTICE '=========================================';
    RAISE NOTICE 'Starting SILVER layer load';
    RAISE NOTICE '=========================================';

    ----------------------------------------------------------
    -- CRM: CUSTOMER
    ----------------------------------------------------------
    start_time := clock_timestamp();

    RAISE NOTICE 'Truncating: silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'Loading: silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info
    SELECT
        cst_id,
        TRIM(cst_key),
        INITCAP(cst_firstname),
        INITCAP(cst_lastname),
        cst_material_status,
        cst_gndr,
        cst_create_date,
        CURRENT_TIMESTAMP
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL;

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Loaded crm_cust_info in % seconds', duration_seconds;

    ----------------------------------------------------------
    -- CRM: PRODUCT
    ----------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.crm_prd_info;

    INSERT INTO silver.crm_prd_info
    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        CURRENT_TIMESTAMP
    FROM bronze.crm_prd_info;

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Loaded crm_prd_info in % seconds', duration_seconds;

    ----------------------------------------------------------
    -- CRM: SALES
    ----------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.crm_sales_details;

    INSERT INTO silver.crm_sales_details
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        CURRENT_TIMESTAMP
    FROM bronze.crm_sales_details;

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Loaded crm_sales_details in % seconds', duration_seconds;

    ----------------------------------------------------------
    -- ERP: CUSTOMER
    ----------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.erp_cust_az12;

    INSERT INTO silver.erp_cust_az12
    SELECT
        cid,
        bdate,
        gen,
        CURRENT_TIMESTAMP
    FROM bronze.erp_cust_az12;

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Loaded erp_cust_az12 in % seconds', duration_seconds;

    ----------------------------------------------------------
    -- ERP: LOCATION
    ----------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.erp_loc_a101;

    INSERT INTO silver.erp_loc_a101
    SELECT
        cid,
        cntry,
        CURRENT_TIMESTAMP
    FROM bronze.erp_loc_a101;

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Loaded erp_loc_a101 in % seconds', duration_seconds;

    ----------------------------------------------------------
    -- ERP: CATEGORY
    ----------------------------------------------------------
    start_time := clock_timestamp();

    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    INSERT INTO silver.erp_px_cat_g1v2
    SELECT
        id,
        cat,
        subcat,
        maintenance,
        CURRENT_TIMESTAMP
    FROM bronze.erp_px_cat_g1v2;

    end_time := clock_timestamp();
    duration_seconds := EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE 'Loaded erp_px_cat_g1v2 in % seconds', duration_seconds;

    ----------------------------------------------------------
    -- FINISH
    ----------------------------------------------------------
    RAISE NOTICE '=========================================';
    RAISE NOTICE 'SILVER layer load completed successfully';
    RAISE NOTICE '=========================================';

END;
$$;
