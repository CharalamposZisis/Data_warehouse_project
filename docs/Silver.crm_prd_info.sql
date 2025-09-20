-- Fix crm_prd_info

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


select * from silver.crm_prd_info;