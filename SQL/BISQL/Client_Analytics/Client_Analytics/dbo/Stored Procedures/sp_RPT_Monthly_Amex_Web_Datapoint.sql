CREATE PROCEDURE [dbo].[sp_RPT_Monthly_Amex_Web_Datapoint]
AS
--SELECT TOP 10 *
--FROM CLIENT_ANALYTICS.dbo.RPT_email_guid
DECLARE @start_mo DATE = '1/1/21';
        --,
       ---- @end_mo DATE = '10/1/21';
	    --@end_mo DATE =  DATEADD(month, DATEDIFF(month, 0, GETDATE() - 1), 0);                             -------------------Start of current month

---WITH client AS (
DROP TABLE	IF EXISTS	#TMP_client
SELECT * INTO  #TMP_client  FROM
(
SELECT b.level, a.clientid FROM DW_MSTR_DM.dbo.DimClient a (NOLOCK) 
LEFT JOIN 
(select 'Primary' as level, 'R1GQM' as clientid union
select 'Primary' as level, 'R1JQM' as clientid union
select 'Primary' as level, 'R1KQM' as clientid union
select 'Primary' as level, 'R1JRM' as clientid union
select 'Primary' as level, 'R1LRM' as clientid union
select 'Primary' as level, 'R1LQM' as clientid union
select 'Primary' as level, 'R1FQM' as clientid union
select 'Primary' as level, 'R1X7M' as clientid union
select 'Primary' as level, 'R1X3M' as clientid union
select 'Primary' as level, 'R1KEM' as clientid union
select 'Primary' as level, 'R1X5M' as clientid union
select 'Primary' as level, 'R1W7M' as clientid union
select 'Primary' as level, 'R1W3M' as clientid union
select 'Primary' as level, 'R1KZM' as clientid union
select 'Primary' as level, 'R1JZM' as clientid union
select 'Primary' as level, 'R1QZM' as clientid union
select 'Primary' as level, 'R15ZM' as clientid union
select 'Primary' as level, 'R1JWM' as clientid union
select 'Primary' as level, 'R1RZM' as clientid union
select 'Primary' as level, 'R1KWM' as clientid union
select 'Primary' as level, 'R1RWM' as clientid union
select 'Primary' as level, 'R1W5M' as clientid union
select 'Primary' as level, 'R1J2M' as clientid union
select 'Primary' as level, 'R1FVM' as clientid union
select 'Primary' as level, 'R1J8M' as clientid union
select 'Primary' as level, 'R1J9M' as clientid union
select 'Primary' as level, 'R1L8M' as clientid union
select 'Primary' as level, 'R1L9M' as clientid union
select 'Primary' as level, 'R1K6M' as clientid union
select 'Primary' as level, 'R1F7M' as clientid union
select 'Primary' as level, 'R1G7M' as clientid union
select 'Primary' as level, 'R1K8M' as clientid union
select 'Primary' as level, 'R1X1M' as clientid union
select 'Primary' as level, 'R1W1M' as clientid union
select 'Primary' as level, 'R1X2M' as clientid union
select 'Primary' as level, 'R1W2M' as clientid union
select 'Primary' as level, 'R1MQM' as clientid union
select 'Primary' as level, '111LHIR' as clientid union
select 'Primary' as level, '111LLOR' as clientid union
select 'Primary' as level, '111BHIR' as clientid union
select 'Primary' as level, '111BMDR' as clientid union
select 'Primary' as level, '111LMDR' as clientid union
select 'Primary' as level, '111CHBR' as clientid union
select 'Primary' as level, '111CLBR' as clientid union
select 'Primary' as level, '11MLHIR' as clientid union
select 'Primary' as level, '11MLMDR' as clientid union
select 'Primary' as level, '11MLLOR' as clientid union
select 'Primary' as level, '11MOLOR' as clientid union
select 'Secondary' as level, 'R24ZM' as clientid union
select 'Secondary' as level, 'R24WM' as clientid union
select 'Secondary' as level, 'R25ZM' as clientid union
select 'Secondary' as level, 'R2HDM' as clientid union
select 'Secondary' as level, 'R2GBM' as clientid union
select 'Secondary' as level, 'R201M' as clientid union
select 'Secondary' as level, '112LOWR' as clientid union
select 'Tertiary' as level, 'R3JBM' as clientid
)  b
ON a.clientid = b.clientid
WHERE a.SourceSystem = 'AMEX Latitude'

) AS client
--),

--inv AS (
DROP TABLE	IF EXISTS	 #TMP_inv
SELECT * INTO #TMP_inv FROM
(
SELECT ap.rpt_month
       , ap.ClientId
	   , SUM(ap.accounts) AS accounts
FROM CLIENT_ANALYTICS.dbo.RPT_Active_Inv_Performance ap (NOLOCK)
       JOIN
     #TMP_client cl ON ap.ClientId=cl.clientid
	 ---client cl ON ap.ClientId=cl.clientid
	 AND ap.KeySourceSystem = 3
WHERE ap.rpt_month >= @start_mo --AND @end_mo--'1/1/21' AND '3/1/21'
GROUP BY ap.rpt_month
       , ap.ClientId
) AS inv
---),
---SELECT * FROm #TMP_inv ORDER BY rpt_month desc

---web AS (
DROP TABLE	IF EXISTS	#TMP_web
SELECT * INTO #TMP_web FROM
(
SELECT cl.clientid
       , cl.level
       , dd.MonthDate AS SendMonth
	   , COUNT(CASE WHEN w.[Key]='reference_number_login_succeeded' THEN w.MetricId END) AS login_attempts
	   , COUNT(CASE WHEN w.[Key]='pin_login_succeeded' THEN w.MetricId END) AS succesful_logins
    ------  , CASE WHEN COUNT(CASE WHEN w.[Key]='reference_number_login_succeeded' THEN w.MetricId END)=0 THEN 0 else
	------     CAST(COUNT(CASE WHEN w.[Key]='pin_login_succeeded' THEN w.MetricId END) AS FLOAT)
	------     /
	------	  COUNT(CASE WHEN w.[Key]='reference_number_login_succeeded' THEN w.MetricId END) 
	------	  END AS login_rate
	   , COUNT(CASE WHEN w.Payments>0 THEN w.MetricId end) AS payers
    ------   , CASE WHEN COUNT(CASE WHEN w.[Key]='reference_number_login_succeeded' THEN w.MetricId END)=0 THEN 0 else
	------     CAST(COUNT(DISTINCT CASE WHEN w.Payments>0 THEN w.MetricId end) AS FLOAT)
	------      /
	------	 CAST(COUNT(DISTINCT CASE WHEN w.[Key]='reference_number_login_succeeded' THEN w.MetricId END) AS FLOAT) 
	------	 END AS unq_payer_rt
	   , SUM(w.Payments) AS payments

FROM DW_MSTR_DM.dbo.Web_Waterfall_Metrics_Data_Client w (NOLOCK)
       JOIN
     #TMP_client cl ON w.clientid=cl.clientid
	--- client cl ON w.clientid=cl.clientid
	AND LEFT(w.ReferenceNumber,3) = '004'
	   JOIN  
     DW_MSTR_DM.dbo.DimDate dd (NOLOCK)
	 ON CAST(w.CapturedOn AS DATE)=dd.CalendarDate
WHERE w.CapturedOn >= @start_mo --AND @end_mo--'1/1/21' AND '3/1/21'
GROUP BY cl.clientid
       , cl.level
       , dd.MonthDate

------) 
) AS web
---SELECT * FROm #TMP_web

TRUNCATE TABLE  CLIENT_ANALYTICS.[dbo].[RPT_AXP_web_report]
INSERT INTO CLIENT_ANALYTICS.[dbo].[RPT_AXP_web_report]
SELECT web.clientid
       , web.level
	   , web.SendMonth
	   , web.login_attempts
	   , web.succesful_logins
	  ------ , web.login_rate
	  ------ , CAST(web.login_attempts AS FLOAT)/CAST(inv.accounts AS FLOAT) AS web_engagement
	   , NULL AS selected_offer
	   , web.payers
	  ------ , web.unq_payer_rt
	   , web.payments
	   , NULL AS pct_contribution
	   ,inv.accounts
	   ,GETDATE() AS InsertDate 
FROM #TMP_web AS web
---FROM web 
      JOIN
	  #TMP_inv AS inv
    --- inv
	  ON web.clientid=inv.ClientId
	        AND web.SendMonth=inv.rpt_month
ORDER BY 3,1,2



--SELECT [key], COUNT(*)
--FROM DW_MSTR_MR.dbo.Web_Waterfall_Metrics_Data_Client
--WHERE CapturedOn>='4/1/21'
--      AND ClientParent LIKE 'Ameri%'
--GROUP BY [Key]
--ORDER BY 1

--SELECT TOP 10 *
--FROM DW_MSTR_MR.dbo.Web_Waterfall_Metrics_Data_Client
GO


