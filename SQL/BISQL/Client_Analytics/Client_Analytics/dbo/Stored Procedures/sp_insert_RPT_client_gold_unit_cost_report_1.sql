



CREATE   PROCEDURE [dbo].[sp_insert_RPT_client_gold_unit_cost_report] 

	@report_date Date = NULL
	

AS

BEGIN

IF @report_date IS NULL 
SET @report_date = DATEADD(m, DATEDIFF(m, 0, GETDATE()), 0) --first day of month
ELSE 
SET @report_date = DATEADD(m, DATEDIFF(m, 0, @report_date), 0)
 
		select lcu.CUSTOMER_ID
				, lcu.CLIENT_ID
				, lcu.LIST_DATE
				, max(obf.INITIAL_BALANCE) as INITIAL_BALANCE
				--, sum(pf.PYMT_AMOUNT) as PYMT_AMOUNT --Use payment_amt_applied instead
				, sum(pf.PAYMENT_AMT_APPLIED) as PAYMENT_AMT_APPLIED --has the sign reversed for negative payments (NSF/etc) 
				, sum(pf.AMT_DUE_AGENCY) as AMT_DUE_AGENCY 
				, max(CASE 
				  WHEN lcu.CLIENT_ID IN ('GOLD1', 'GOLD2', 'GOLD7') THEN '1. Primary'
				  WHEN lcu.CLIENT_ID IN ('GOLD3', 'GOLD4', 'GOLD5') THEN '2. Secondary'
				  WHEN lcu.CLIENT_ID IN ('GOLD6') THEN '3. Tertiary'
				  WHEN lcu.CLIENT_ID IN ('GOLD8') THEN '5. OOS'
				  END) AS Market
				, max(CASE WHEN lcu.CLIENT_ID = 'GOLD7' THEN '4. Midprime'
				  ELSE 'Other' END)			  
				  AS Midprime
				, max(CASE 
				  WHEN DATEDIFF(MONTH,lcu.LIST_DATE,@report_date) <= 6 THEN 'Month 1-6'
				  WHEN DATEDIFF(MONTH,lcu.LIST_DATE,@report_date) BETWEEN 7 AND 12 THEN 'Month 7-12'
				  WHEN DATEDIFF(MONTH,lcu.LIST_DATE,@report_date) BETWEEN 13 AND 18 THEN 'Month 13-18'
				  WHEN DATEDIFF(MONTH,lcu.LIST_DATE,@report_date) BETWEEN 19 AND 24 THEN 'Month 19-24'
				  WHEN DATEDIFF(MONTH,lcu.LIST_DATE,@report_date) BETWEEN 25 AND 30 THEN 'Month 25-30'
				  ELSE 'Other'
				  END) AS TimeInterval
				, max(CASE 
				  WHEN DATEDIFF(MONTH,lcu.LIST_DATE,@report_date) <= 30 THEN 'Total'
				  ELSE 'Other'
				  END) AS TimeInterval_Total 
				, CAST (dateadd(day,datediff(day,1,@report_date),0) as DATE) as report_date  
				
		INTO #customers		   	     
		FROM DW_MSTR_DM.dbo.LU_CUSTOMER lcu (NOLOCK) 
				left outer join	
			DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT obf (nolock) on lcu.CUSTOMER_ID=obf.CUSTOMER_ID		
				LEFT OUTER JOIN	
			DW_MSTR_DM.dbo.PAYMENT_FACT pf (nolock) ON lcu.CUSTOMER_ID=pf.CUSTOMER_ID
													AND Pf.PYMT_TYPE NOT IN ('DBJ','CRJ','PCK','CAN')	

		WHERE lcu.CLIENT_ID IN ('GOLD1', 'GOLD2', 'GOLD3', 'GOLD4', 'GOLD5', 'GOLD6', 'GOLD7', 'GOLD8')
							--AND Pf.PYMT_TYPE NOT IN ('DBJ','CRJ','PCK','CAN') *this only includes payers, so move to join clause to include non-payers
							AND lcu.LIST_DATE < @report_date
							AND lcu.CUSTOMER_ID not like '99999999%'
		GROUP BY lcu.CUSTOMER_ID, lcu.CLIENT_ID, lcu.LIST_DATE


		
		---DELETE existing records from CLIENT_ANALYTICS.dbo.RPT_client_gold_unit_cost_report

		DELETE FROM CLIENT_ANALYTICS.dbo.RPT_client_gold_unit_cost_report
		WHERE report_date = isnull(@report_date, EOMONTH(DATEADD(MONTH,-1,GETDATE())))     




		-- Summary by Market
		
		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_gold_unit_cost_report (
		  Market
		, TimeInterval
		, accts_placed 
		, dollar_placed
		, dollar_collected 
		, avg_balance 
		, gross_liq_rate 
		, cost 
		, net_liq_rate 
		, unit_cost 
		, report_yrmo
		, report_date)

		SELECT Market, TimeInterval
				, COUNT(CUSTOMER_ID) as accts_placed
				, sum(INITIAL_BALANCE) as dollar_placed
				, sum(PAYMENT_AMT_APPLIED) as dollar_collected 
				, sum(INITIAL_BALANCE) / count(CUSTOMER_ID) as avg_balance
				, sum(PAYMENT_AMT_APPLIED) / sum(INITIAL_BALANCE) as gross_liq_rate
				, sum(AMT_DUE_AGENCY) as cost
				, (sum(PAYMENT_AMT_APPLIED) - sum(AMT_DUE_AGENCY)) / sum(INITIAL_BALANCE) as net_liq_rate
				, (sum(AMT_DUE_AGENCY) / sum(PAYMENT_AMT_APPLIED)) as unit_cost
				, CONCAT (datepart (yy,report_date), format(report_date, 'MM')) as report_yrmo
				, report_date

		FROM #customers
		WHERE TimeInterval <> 'Other'
		Group by Market, TimeInterval, report_date
		Order by Market, TimeInterval, report_date



		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_gold_unit_cost_report (
		  Market
		, TimeInterval
		, accts_placed 
		, dollar_placed
		, dollar_collected 
		, avg_balance 
		, gross_liq_rate 
		, cost 
		, net_liq_rate 
		, unit_cost 
		, report_yrmo
		, report_date)

		SELECT Market
				, TimeInterval = 'Total'
				, COUNT(CUSTOMER_ID) as accts_placed
				, sum(INITIAL_BALANCE) as dollar_placed
				, sum(PAYMENT_AMT_APPLIED) as dollar_collected 
				, sum(INITIAL_BALANCE) / count(CUSTOMER_ID) as avg_balance
				, sum(PAYMENT_AMT_APPLIED) / sum(INITIAL_BALANCE) as gross_liq_rate
				, sum(AMT_DUE_AGENCY) as cost
				, (sum(PAYMENT_AMT_APPLIED) - sum(AMT_DUE_AGENCY)) / sum(INITIAL_BALANCE) as net_liq_rate
				, (sum(AMT_DUE_AGENCY) / sum(PAYMENT_AMT_APPLIED)) as unit_cost
				, CONCAT (datepart (yy,report_date), format(report_date, 'MM')) as report_yrmo
				, report_date
		
		FROM #customers
		WHERE TimeInterval <> 'Other'
		Group by Market, report_date
		Order by Market, report_date	


		-- Summary for Mid-Prime
		
		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_gold_unit_cost_report (
		  Market
		, TimeInterval
		, accts_placed 
		, dollar_placed
		, dollar_collected 
		, avg_balance 
		, gross_liq_rate 
		, cost 
		, net_liq_rate 
		, unit_cost 
		, report_yrmo
		, report_date)

		SELECT Midprime as Market
				, TimeInterval
				, COUNT(CUSTOMER_ID) as accts_placed
				, sum(INITIAL_BALANCE) as dollar_placed
				, sum(PAYMENT_AMT_APPLIED) as dollar_collected 
				, sum(INITIAL_BALANCE) / count(CUSTOMER_ID) as avg_balance
				, sum(PAYMENT_AMT_APPLIED) / sum(INITIAL_BALANCE) as gross_liq_rate
				, sum(AMT_DUE_AGENCY) as cost
				, (sum(PAYMENT_AMT_APPLIED) - sum(AMT_DUE_AGENCY)) / sum(INITIAL_BALANCE) as net_liq_rate
				, (sum(AMT_DUE_AGENCY) / sum(PAYMENT_AMT_APPLIED)) as unit_cost
				, CONCAT (datepart (yy,report_date), format(report_date, 'MM')) as report_yrmo
				, report_date

		FROM #customers 
		WHERE Midprime = '4. Midprime'
		Group by Midprime, TimeInterval, report_date
		Order by Midprime, TimeInterval, report_date


	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_gold_unit_cost_report (
		  Market
		, TimeInterval
		, accts_placed 
		, dollar_placed
		, dollar_collected 
		, avg_balance 
		, gross_liq_rate 
		, cost 
		, net_liq_rate 
		, unit_cost 
		, report_yrmo
		, report_date)

		SELECT Midprime as Market
				, TimeInterval = 'Total' 
				, COUNT(CUSTOMER_ID) as accts_placed
				, sum(INITIAL_BALANCE) as dollar_placed
				, sum(PAYMENT_AMT_APPLIED) as dollar_collected 
				, sum(INITIAL_BALANCE) / count(CUSTOMER_ID) as avg_balance
				, sum(PAYMENT_AMT_APPLIED) / sum(INITIAL_BALANCE) as gross_liq_rate
				, sum(AMT_DUE_AGENCY) as cost
				, (sum(PAYMENT_AMT_APPLIED) - sum(AMT_DUE_AGENCY)) / sum(INITIAL_BALANCE) as net_liq_rate
				, (sum(AMT_DUE_AGENCY) / sum(PAYMENT_AMT_APPLIED)) as unit_cost
				, CONCAT (datepart (yy,report_date), format(report_date, 'MM')) as report_yrmo
				, report_date

		FROM #customers 
		WHERE Midprime = '4. Midprime'
		Group by Midprime, report_date
		Order by Midprime, report_date

END;