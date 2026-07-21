


CREATE PROCEDURE [dbo].[usp_RPT_Envision_Placement]
AS 


TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Envision_Placement 

DROP TABLE IF EXISTS #TEMP_CUST

SELECT dd.MonthYear as Month_Year,dcl.ClientId,dcl.clientstreamid,
	dcu.KeyCustomer dcu_KeyCustomer,dcu.CustomerAddress1,dcu.SSN,
	dcu.ClientOpenDate,dcu.ListDate,dce.KeyCustomer dce_KeyCustomer 
INTO #TEMP_CUST 
FROM [DW_MSTR_DM].dbo.DimCustomer dcu WITH (NOLOCK) 
	JOIN [DW_MSTR_DM].[dbo].[DimClient] dcl WITH (NOLOCK) ON dcu.ClientId=dcl.ClientId
	JOIN [DW_MSTR_DM].[dbo].[DimDate] dd WITH (NOLOCK) ON dd.Calendardate=dcu.listDate
	LEFT JOIN [DW_MSTR_DM].[dbo].DimCustomerEmail dce WITH (NOLOCK) ON dcu.KeyCustomer=dce.KeyCustomer
WHERE dcl.ClientStreamId LIKE 'IMRTI%'  
--WHERE dcl.ClientParent LIKE 'PRT%'  

CREATE INDEX #t_temp_cust_keyCustomer ON #temp_cust(dcu_KeyCustomer)
 
DROP TABLE IF EXISTS #TEMP_FINAL

SELECT t.*, rp.PhoneNumber 
INTO #TEMP_FINAL 
FROM #temp_cust t 
	LEFT JOIN [DW_MSTR_DM].[dbo].[RadiusPhone] rp WITH (NOLOCK) ON t.dcu_Keycustomer = rp.KeyCustomer

INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Envision_Placement
    (
	 [Month_Year]
	  ,[ClientStreamId]
      ,[class]
      ,[Accounts]
      ,[PhoneNumbers]
      ,[Adresses]
      ,[SSNs]
      ,[less_than_3_months]
      ,[three_six_months]
      ,[six_nine_months]
      ,[nine_12_months]
      ,[twelve_fifteen_months]
      ,[fifteen_eighteen_months]
	  ,[eighteen_twentyone_months]
	  ,[twentyone_twentyfour_months]
      ,[more_24_months]
      ,[count_w_email]
      ,[pct_w_email]
	)
SELECT T.Month_Year as Month_Year, t.clientstreamid,
CASE WHEN t.clientstreamid in ('IMRTI') THEN 'Primes'
     WHEN t.clientstreamid in ('IMRTI2','IMRTIA','IMRTI2A')THEN 'Seconds'
     WHEN t.clientstreamid in ('IMRTI3A') THEN 'Tert'
	 ELSE 'Other' END  AS class 
	 ,count(DISTINCT t.dcu_KeyCustomer) Accounts
	,count(distinct (case when  LEN(T.PhoneNumber)=10 then t.dcu_Keycustomer end)) PhoneNumbers
	,count(distinct (case when  LEN(t.CustomerAddress1)>5 then t.dcu_Keycustomer end)) Adresses
	,count(distinct (case when  LEN(t.SSN)>=4 then t.dcu_Keycustomer end)) SSNs
	,count( distinct case when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) < 3 then t.dcu_Keycustomer  end ) as less_than_3_months
	,count( distinct case  when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 3 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<6 then t.dcu_Keycustomer end) as three_six_months
	,count( distinct case when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 6 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<9 then t.dcu_Keycustomer end) as six_nine_months
	,count( distinct case when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 9 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<12 then t.dcu_Keycustomer end) as nine_12_months
	,count( distinct case  when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 12 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<15 then t.dcu_Keycustomer end) as twelve_fifteen_months
	,count( distinct case  when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 15 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<18 then t.dcu_Keycustomer end) as fifteen_eighteen_months
	,count( distinct case  when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 18 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<21 then t.dcu_Keycustomer end) as eighteen_twentyone_months
	,count( distinct case  when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >= 21 and DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate)<24 then t.dcu_Keycustomer end) as twentyone_twentyfour_months
	,count( distinct case when DATEDIFF(MONTH,t.ClientOpenDate,t.ListDate) >=24 then t.dcu_KeyCustomer end) as more_24_months
	,count(distinct t.dce_Keycustomer) as count_w_email
	,cast(count(distinct t.dce_KeyCustomer) as float) / 
	cast(count(distinct t.dcu_keycustomer) as float) as pct_w_email 
FROM #TEMP_FINAL T 
GROUP BY t.Month_Year, t.clientstreamid,
CASE WHEN t.clientstreamid in ('IMRTI') THEN 'Primes'
     WHEN t.clientstreamid in ('IMRTI2','IMRTIA','IMRTI2A')THEN 'Seconds'
     WHEN t.clientstreamid in ('IMRTI3A') THEN 'Tert'
	 ELSE 'Other' END