/*
===============================================================================
Stored Procedure: bronze.load_bronze
Purpose:
    Loads raw source data from external CSV files into the Bronze Layer tables.

Description:
    This procedure performs a full refresh of all Bronze tables by:
      - Truncating each target table before loading.
      - Performing BULK INSERT operations from local CSV source files.
      - Ensuring consistent, clean, and repeatable ingestion into the Bronze layer.

Notes:
    - The procedure does not take any parameters.
    - It does not return any result sets.
    - Intended for use in ETL/ELT pipelines as a raw data ingestion step.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- ==================================================
        --                   START HEADER
        -- ==================================================
        PRINT '=================================================';
        PRINT '             Starting Bronze Layer Load          ';
        PRINT '=================================================';
        PRINT '';

        -- ==================================================
        --                   CRM TABLES
        -- ==================================================
        PRINT '-------------------------------------------------';
        PRINT '                  Loading CRM Tables            ';
        PRINT '-------------------------------------------------';

        -- Customer Info
        PRINT '';
        PRINT ' * Truncating Table: CRM.Customer Info ';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT ' * Loading CRM.Customer Info ';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        PRINT ' * Finished loading CRM.Customer Info ';
        PRINT '-------------------------------------------------';

        -- Product Info
        PRINT '';
        PRINT ' * Truncating Table: CRM.Product Info ';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT ' * Loading CRM.Product Info ';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        PRINT ' * Finished loading CRM.Product Info ';
        PRINT '-------------------------------------------------';

        -- Sales Details
        PRINT '';
        PRINT ' * Truncating Table: CRM.Sales Details ';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT ' * Loading CRM.Sales Details ';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        PRINT ' * Finished loading CRM.Sales Details ';
        PRINT '-------------------------------------------------';

        -- ==================================================
        --                   ERP TABLES
        -- ==================================================
        PRINT '';
        PRINT '-------------------------------------------------';
        PRINT '                  Loading ERP Tables            ';
        PRINT '-------------------------------------------------';

        -- Customer Data (CUST_AZ12)
        PRINT '';
        PRINT ' * Truncating Table: ERP.Customer Data (CUST_AZ12) ';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT ' * Loading ERP.Customer Data (CUST_AZ12) ';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        PRINT ' * Finished loading ERP.Customer Data (CUST_AZ12) ';
        PRINT '-------------------------------------------------';

        -- Location Data (LOC_A101)
        PRINT '';
        PRINT ' * Truncating Table: ERP.Location Data (LOC_A101) ';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT ' * Loading ERP.Location Data (LOC_A101) ';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        PRINT ' * Finished loading ERP.Location Data (LOC_A101) ';
        PRINT '-------------------------------------------------';

        -- Product Category (PX_CAT_G1V2)
        PRINT '';
        PRINT ' * Truncating Table: ERP.Product Category (PX_CAT_G1V2) ';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT ' * Loading ERP.Product Category (PX_CAT_G1V2) ';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        PRINT ' * Finished loading ERP.Product Category (PX_CAT_G1V2) ';
        PRINT '=================================================';
        PRINT '         Bronze Layer Data Load Completed    ';
        PRINT '=================================================';

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
        PRINT '       !ERROR: Bronze Layer Load Failed!';
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';

        THROW;
    END CATCH
END;
GO
