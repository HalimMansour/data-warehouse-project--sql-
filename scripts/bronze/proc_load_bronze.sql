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

        /* -------------------------------------
           CRM: Customer Info
        ------------------------------------- */
        TRUNCATE TABLE bronze.crm_cust_info;

        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );


        /* -------------------------------------
           CRM: Product Info
        ------------------------------------- */
        TRUNCATE TABLE bronze.crm_prd_info;

        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );


        /* -------------------------------------
           CRM: Sales Details
        ------------------------------------- */
        TRUNCATE TABLE bronze.crm_sales_details;

        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );


        /* -------------------------------------
           ERP: Customer Data (CUST_AZ12)
        ------------------------------------- */
        TRUNCATE TABLE bronze.erp_cust_az12;

        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );


        /* -------------------------------------
           ERP: Location Data (LOC_A101)
        ------------------------------------- */
        TRUNCATE TABLE bronze.erp_loc_a101;

        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );


        /* -------------------------------------
           ERP: Product Category (PX_CAT_G1V2)
        ------------------------------------- */
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        THROW;  -- returns full error to caller
    END CATCH
END;
GO

