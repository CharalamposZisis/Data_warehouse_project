/*
============================================
Stored Procedure : Load Silver Layer (Bronze -> Silver)
===========================================
Script Purpose:
  This stored procedure creates the ETL (Extract Transform Load) process to populate the Silver schema from the bronze schema.
  It performs the following actions:
  - Truncates the Silver tables.
  - Insert transformed and cleansed data from Bronze into silver tables.
  Usage example EXEC silver.load_silver;
*/ 



-- Truncate and insert ALL Data in silver Tables
-- silver.crm_cust_info
CREATE OR ALTER PROCEDURE silver.load_silver as 
	begin
	declare @start_br_layer_time datetime, @end_br_layer_time datetime,@start_time datetime, @endtime datetime;
	set @start_br_layer_time = GETDATE();
	begin try
		print('=================================');
		print ('loading Silver Layer');
		print('=================================');
		print('--------------------------------');
		print('Loading CRM Tables');

		set @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE: silver.crm_cust_info');
		truncate table silver.crm_cust_info
		print('Inserting data into: silver.crm_cust_info');
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select 
			cst_id,
			cst_key ,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case when upper(trim(cst_marital_status)) = 'S' then 'Single'
				when upper(trim(cst_marital_status)) = 'M' then 'Married'
				else 'n/a'
			end cst_marital_status,
			case when upper(trim(cst_gndr)) = 'F' then 'Female'
				when upper(trim(cst_gndr)) = 'M' then 'Male'
				else 'n/a'
			end cst_gndr,
		cst_create_date 
		from
		(select 
		*,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info) t 
		where flag_last = 1; 

		set @endtime = GETDATE();
		print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
		print('-------------------');


		-- silver.crm_prd_info
		set @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE: silver.crm_prd_info');
		truncate table silver.crm_prd_info
		print('Inserting data into: silver.crm_prd_info');
		insert into silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
			select 
			prd_id,
			replace(SUBSTRING(prd_key,1,5), '-','_') as cat_id, -- filter to keep only the first 5 charachters and replace the '-' with '_'
			SUBSTRING(prd_key,7,len(prd_key)) as prd_key,
			prd_nm ,
			isnull(prd_cost,0) as prd_cost ,
			case upper(trim(prd_line))
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'T' then 'Touring'
				when 'S' then 'Other Sales'
				else 'n/a'
			end as prd_line,
			prd_start_dt,
			DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt_test -- Data enrichment: we add new, relevant data to enhance the dataset
			from bronze.crm_prd_info;
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');


		-- silver.crm_sales_details
		set @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE: silver.crm_sales_details');
		truncate table silver.crm_sales_details
		print('Inserting data into: silver.crm_sales_details');
		insert into silver.crm_sales_details ( 
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt ,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_price,
		sls_quantity)
			SELECT 
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
				 else cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,
			case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
				 else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,
			case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
				 else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,
			case when sls_sales != abs(sls_price)*sls_quantity or sls_sales is null or sls_sales <=0
				then abs(sls_price)*sls_quantity 
			else sls_sales
			end as sls_sales,
			case when sls_price is null or sls_price <=0 
				 then sls_sales/nullif(sls_quantity,0)
			   else sls_price
			end as sls_price,
			sls_quantity
			from bronze.crm_sales_details;
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');



		-- silver.erp_px_cat_g1v2
		set @start_time = GETDATE();
		PRINT('-------------------------');
		PRINT('Loading ERP Tables');
		PRINT('>> TRUNCATING TABLE: silver.erp_px_cat_g1v2');
		truncate table silver.erp_px_cat_g1v2
		print('Inserting data into: silver.erp_px_cat_g1v2');
		insert into silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2
		set @endtime = GETDATE();
		print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
		print('-------------------');


		-- erp_cust_az12
		set @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE: silver.erp_cust_az12');
		truncate table silver.erp_cust_az12
		print('Inserting data into: silver.erp_cust_az12');
		insert into silver.erp_cust_az12 (cid,bdate,gen)
			select 
			case when cid like 'NAS%' then substring(cid, 4, len(cid)) 
				else cid end as cid,
			case when bdate > getdate() then null 
				 else bdate end as bdate,
			case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
				 when upper(trim(gen)) in ('M','MALE') then 'Male'
				 else 'n/a' 
				 END AS gen
			from bronze.erp_cust_az12;
			set @endtime = GETDATE();f
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');


		-- silver.erp_loc_a101
		set @start_time = GETDATE();
		PRINT('>> TRUNCATING TABLE: silver.erp_loc_a101');
		truncate table silver.erp_loc_a101
		print('Inserting data into: silver.erp_loc_a101');
		insert into silver.erp_loc_a101 (
		cid,
		cntry
		)
			select 
			replace(cid, '-', '') cid,
			case when isnull(cntry,'') = '' then 'n/a'
				when cntry in ('US','USA') then 'United States'
				when cntry like 'DE' then 'Germany'
				else cntry
			end as cntry
			from bronze.erp_loc_a101;
			set @endtime = GETDATE();
			print '>> Loading time:' + cast(Datediff(second, @start_time, @endtime) as nvarchar) + ' seconds';
			print('-------------------');
			Print 'The loading time for the Silver layer is:' + cast(datediff(second, @start_br_layer_time, @end_br_layer_time) as nvarchar) + ' seconds';
		end try 
		begin catch
			print '========================================='
			print 'ERROR OCCURED DURING LOADED SILVER LAYER'
			PRINT 'Error Message' + error_message();
			print 'Error Message' + cast (error_number() as nvarchar);
			print 'Error Message' + cast(error_state() as nvarchar); 
			print '========================================='
		end catch
	end;
go


EXEC silver.load_silver;
go
