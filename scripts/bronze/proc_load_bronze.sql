CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    -- Optional: suppress "rows affected" messages
    -- SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- ==================================================
        --                   START HEADER
        -- ==================================================
        PRINT '=================================================';
        PRINT '             Starting Bronze Layer Load          ';
        PRINT '             Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT '=================================================';
        PRINT '';

        -- ==================================================
        --                   CRM TABLES
        -- ==================================================
        PRINT '-------------------------------------------------';
        PRINT '                  Loading CRM Tables            ';
        PRINT '-------------------------------------------------';

        DECLARE @StartTime DATETIME2, @EndTime DATETIME2, @DurationMS BIGINT;

        -- Customer Info
        PRINT '';
        PRINT ' * Truncating Table: CRM.Customer Info ';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT ' * Loading CRM.Customer Info ';
        SET @StartTime = SYSDATETIME();
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @EndTime = SYSDATETIME();
        SET @DurationMS = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT ' * Finished loading CRM.Customer Info ';
        PRINT '   Duration: ' + CAST(@DurationMS AS VARCHAR(20)) + ' ms';
        PRINT '-------------------------------------------------';

        -- Product Info
        PRINT '';
        PRINT ' * Truncating Table: CRM.Product Info ';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT ' * Loading CRM.Product Info ';
        SET @StartTime = SYSDATETIME();
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @EndTime = SYSDATETIME();
        SET @DurationMS = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT ' * Finished loading CRM.Product Info ';
        PRINT '   Duration: ' + CAST(@DurationMS AS VARCHAR(20)) + ' ms';
        PRINT '-------------------------------------------------';

        -- Sales Details
        PRINT '';
        PRINT ' * Truncating Table: CRM.Sales Details ';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT ' * Loading CRM.Sales Details ';
        SET @StartTime = SYSDATETIME();
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @EndTime = SYSDATETIME();
        SET @DurationMS = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT ' * Finished loading CRM.Sales Details ';
        PRINT '   Duration: ' + CAST(@DurationMS AS VARCHAR(20)) + ' ms';
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
        SET @StartTime = SYSDATETIME();
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @EndTime = SYSDATETIME();
        SET @DurationMS = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT ' * Finished loading ERP.Customer Data (CUST_AZ12) ';
        PRINT '   Duration: ' + CAST(@DurationMS AS VARCHAR(20)) + ' ms';
        PRINT '-------------------------------------------------';

        -- Location Data (LOC_A101)
        PRINT '';
        PRINT ' * Truncating Table: ERP.Location Data (LOC_A101) ';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT ' * Loading ERP.Location Data (LOC_A101) ';
        SET @StartTime = SYSDATETIME();
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @EndTime = SYSDATETIME();
        SET @DurationMS = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT ' * Finished loading ERP.Location Data (LOC_A101) ';
        PRINT '   Duration: ' + CAST(@DurationMS AS VARCHAR(20)) + ' ms';
        PRINT '-------------------------------------------------';

        -- Product Category (PX_CAT_G1V2)
        PRINT '';
        PRINT ' * Truncating Table: ERP.Product Category (PX_CAT_G1V2) ';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT ' * Loading ERP.Product Category (PX_CAT_G1V2) ';
        SET @StartTime = SYSDATETIME();
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Work\Study\SQL\data_warehouse_datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @EndTime = SYSDATETIME();
        SET @DurationMS = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
        PRINT ' * Finished loading ERP.Product Category (PX_CAT_G1V2) ';
        PRINT '   Duration: ' + CAST(@DurationMS AS VARCHAR(20)) + ' ms';
        PRINT '=================================================';
        PRINT '         Bronze Layer Data Load Completed       ';
        PRINT '         Completion Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
        PRINT '=================================================';

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';
        PRINT '       ERROR: Bronze Layer Load Failed!       ';
        PRINT '       Error Message: ' + ERROR_MESSAGE();
        PRINT '       Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '       Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!';

        THROW;
    END CATCH
END;
GO
