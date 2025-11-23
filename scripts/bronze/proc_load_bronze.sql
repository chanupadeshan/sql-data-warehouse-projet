
--- CRM Data Load
BULK INSERT bronze.crm_cust_info
FROM '/var/opt/mssql/data/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);


BULK INSERT bronze.crm_prd_info
FROM '/var/opt/mssql/data/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);


BULK INSERT bronze.crm_sales_details
FROM '/var/opt/mssql/data/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);




--- ERP Data Load
BULK INSERT bronze.erp_cust_az12
FROM '/var/opt/mssql/data/CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);


BULK INSERT bronze.erp_loc_a101
FROM '/var/opt/mssql/data/LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);


BULK INSERT bronze.erp_px_cat_g1v2
FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);

