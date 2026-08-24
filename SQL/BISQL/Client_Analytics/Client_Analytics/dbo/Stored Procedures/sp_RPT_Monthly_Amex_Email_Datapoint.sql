CREATE PROCEDURE [dbo].[sp_RPT_Monthly_Amex_Email_Datapoint]
AS
--SELECT TOP 10 *
--FROM CLIENT_ANALYTICS.dbo.RPT_email_guid
DECLARE @start_mo DATE = '1/1/21';
        --,
       --- @end_mo DATE = '9/1/21';
	    --@end_mo DATE =  DATEADD( MONTH, -1, DATEADD(month, DATEDIFF(month, 0, GETDATE() -1), 0));                             -------------------Start of previous month

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
--daily

---em AS (
DROP TABLE	IF EXISTS	#TMP_em
SELECT * INTO #TMP_em FROM
(
SELECT cl.clientid
--       , eg.send_date
       , cl.level
--	   , ae.f7 AS email_type
--	   , cl.clientid
	   --, ae.f3 AS email_header
	   --, ae.f4 AS email_message
	   --, ae.f5 AS email_description
	   --, ae.f6 AS email_version
       , dd.MonthDate AS SendMonth
--	   , eg.LTR
	   --, SUM(eg.requested) AS requested
	   --, SUM(eg.whitelist_scrubbed) AS whitelist_scrubbed
	   --, SUM(eg.optout_scrubbed) AS optout_scrubbed
	   --, SUM(eg.invalid_email_scrubbed) AS invalid_email_scrubbed
	   , SUM(eg.sent) AS sent
	   , COUNT(DISTINCT eg.customerid) AS sent_unq
	   --, SUM(eg.bounced) AS bounced
	   --, SUM(eg.delivered) AS delivered
	   , SUM(eg.unq_opens) AS opened
	   --, SUM(eg.optouts) AS optouts
	   --, SUM(eg.marked_as_spam) AS marked_as_spam
	   --, SUM(eg.clicks) AS clicks
	   --, SUM(eg.payers) AS payers
	   --, SUM(eg.payments) AS payments
	   --, CASE WHEN SUM(eg.requested)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.whitelist_scrubbed+eg.optout_scrubbed+eg.invalid_email_scrubbed) AS FLOAT)/CAST(SUM(eg.requested) AS FLOAT) END AS scrub_rate
	   --, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.bounced) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS bounce_rate
	   --, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.delivered) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS delivered_rate
	   ------, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   ------  CAST(SUM(eg.unq_opens) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS open_rate
	   --, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.optouts) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS optout_rate
	   --, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.marked_as_spam) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS spam_rate
	   --, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.clicks) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS click_rate
	   , SUM(eg.clicks) AS clicks
	   ------, CASE WHEN SUM(eg.unq_opens)=0 THEN 0 ELSE 
	   ------  CAST(SUM(eg.clicks) AS FLOAT)/CAST(SUM(eg.unq_opens) AS FLOAT) END AS click_to_open_rate
	   --, CASE WHEN SUM(eg.sent)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.payers) AS FLOAT)/CAST(SUM(eg.sent) AS FLOAT) END AS pay_rate
	   --, CASE WHEN SUM(eg.delivered)=0 THEN 0 ELSE 
	   --  CAST(SUM(eg.payments) AS FLOAT)/CAST(SUM(eg.delivered) AS FLOAT) END AS dollars_per_delivered
FROM CLIENT_ANALYTICS.dbo.RPT_email_guid eg (NOLOCK)
       JOIN
     #TMP_client cl ON eg.clientid=cl.clientid
	 ---client cl ON eg.clientid=cl.clientid
	AND eg.crm = 'AMEX Latitude'
	   --LEFT JOIN
    -- Analytics_2017.dbo.client_AMEX_email_ltr_meta ae ON eg.LTR=ae.F2
	   JOIN
     DW_MSTR_DM.dbo.DimDate dd (NOLOCK)
	 ON CAST(eg.send_date AS DATE)=dd.CalendarDate
WHERE eg.send_date >= @start_mo --AND @end_mo--'1/1/21' AND '3/1/21'
GROUP BY cl.clientid
--       , eg.send_date
       , cl.level
--	   , ae.f7
--	   , cl.clientid
	   --, ae.f3 AS email_header
	   --, ae.f4 AS email_message
	   --, ae.f5 AS email_description
	   --, ae.f6 AS email_version
       , dd.MonthDate
--	   , eg.LTR
--ORDER BY 1,2,3,4,5,6

------) 
) AS em
---SELECT * FROm #TMP_em

TRUNCATE TABLE CLIENT_ANALYTICS.[dbo].[RPT_AXP_email_report]
INSERT INTO  CLIENT_ANALYTICS.[dbo].[RPT_AXP_email_report]
SELECT em.*
       , inv.accounts
	   -------, CAST(em.opened AS FLOAT)/CAST(inv.accounts AS FLOAT) AS open_engagement
       -------, CAST(em.clicks AS FLOAT)/CAST(inv.accounts AS FLOAT) AS click_engagement
	   , GETDATE() AS InsertDate
FROM #TMP_em AS em
---FROM em
      JOIN
	  #TMP_inv AS inv
 ---    inv 
	 ON em.clientid=inv.ClientId
	 	        AND em.SendMonth=inv.rpt_month
ORDER BY 3,2,1

--SELECT *
--FROM CLIENT_ANALYTICS.dbo.RPT_Active_Inv_Performance
--WHERE ClientId='R2HDM'
--      AND rpt_month='3/1/21'
GO


