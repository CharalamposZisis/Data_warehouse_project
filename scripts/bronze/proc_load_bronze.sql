/*
============================================
Create Bronze Tables
===========================================
Script Purpose:
  This script creates the bronze procedure into the bronze schema from external csv files.
  It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the balk insert (since it alloew many rows into a sql table command to load data from csv files tables.
  Usage example EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze as 
	begin
		declare @start_br_layer_time datetime, @end_br_layer_time datetime,@start_time datetime, @endtime datetime;
		set @start_br_layer_time = GETDATE();
		begin try
			print('=================================');
			print ('loading Bronze Layer');
			print('=================================');
			print('--------------------------------');
			print('Loading CRM Tables');

			set @start_time = GETDATE();
			PRINT('>> TRUNCATING TABLE: bronze.crm_cust_info');
			print('Inserting data into: bronze.crm_cust_info');
			truncate table bronze.crm_cust_info;
			bulk insert bronze.crm_cust_info
			from 'C:\Users\Xaris\Desktop\Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');
		

			set @start_time = GETDATE();
			PRINT('>> TRUNCATING TABLE: bronze.crm_prd_info');
			print('Inserting data into: bronze.crm_prd_info');
			truncate table bronze.crm_prd_info;
			bulk insert bronze.crm_prd_info
			from 'C:\Users\Xaris\Desktop\Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');




			set @start_time = GETDATE();
			PRINT('>> TRUNCATING TABLE: bronze.crm_sales_details');
			print('Inserting data into: bronze.crm_sales_details');
			truncate table bronze.crm_sales_details;
			bulk insert bronze.crm_sales_details
			from 'C:\Users\Xaris\Desktop\Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');


			set @start_time = GETDATE();
			PRINT('-------------------------');
			PRINT('Loading ERP Tables');
			PRINT('>> TRUNCATING TABLE: bronze.erp_cust_az12');
			print('Inserting data into: bronze.erp_cust_az12');
			truncate table bronze.erp_cust_az12;
			bulk insert bronze.erp_cust_az12
			from 'C:\Users\Xaris\Desktop\Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');


			set @start_time = GETDATE();
			PRINT('>> TRUNCATING TABLE: bronze.erp_loc_a101');
			print('Inserting data into: bronze.erp_loc_a101');
			truncate table bronze.erp_loc_a101;
			bulk insert bronze.erp_loc_a101
			from 'C:\Users\Xaris\Desktop\Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');


			set @start_time = GETDATE();
			PRINT('>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2');
			print('Inserting data into: bronze.erp_px_cat_g1v2');
			truncate table bronze.erp_px_cat_g1v2;
			bulk insert bronze.erp_px_cat_g1v2
			from 'C:\Users\Xaris\Desktop\Project\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			with (
				firstrow = 2,
				fieldterminator = ',',
				tablock
			);
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');
			set @end_br_layer_time = GETDATE();
			Print 'The loading time for the bronze layer is:' + cast(datediff(second, @start_br_layer_time, @end_br_layer_time) as nvarchar) + ' seconds';
		end try 
		begin catch
			print '========================================='
			print 'ERROR OCCURED DURING LOADED BRONZE LAYER'
			PRINT 'Error Message' + error_message();
			print 'Error Message' + cast (error_number() as nvarchar);
			print 'Error Message' + cast(error_state() as nvarchar); 
			print '========================================='
		end catch
	end;
go

EXEC bronze.load_bronze;
go
