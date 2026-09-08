






CREATE PROCEDURE [dbo].[sp_ins_Web_Waterfall_Visits_by_Source]

AS
/* 
Object: dbo.sp_ins_Web_Waterfall_Visits_by_Source

Author			Date		Description
Amod Ramugade	08/17/2023	Created

*/
 
 
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @startdate DATETIME; 

	SET @startdate = ISNULL(@startdate,DATEADD(dd,-60,CAST(GETDATE() AS DATE))); 

	DELETE FROM [CLIENT_ANALYTICS].[dbo].[Web_Waterfall_Visits_by_Source]
	WHERE CAST(CapturedDate AS DATE) >= @startdate 
	;


	DROP TABLE IF EXISTS #Web_Waterfall_Metrics_Data_Client
	SELECT * 
	INTO #Web_Waterfall_Metrics_Data_Client
	FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
	WHERE CAST(CapturedOn AS DATE) >= @startdate
	;
	CREATE INDEX #IX_Web_Waterfall_summary_sessionId ON #Web_Waterfall_Metrics_Data_Client(sessionid);




	DROP TABLE IF EXISTS #sms_campaign
	SELECT  DISTINCT SessionId 
	INTO #sms_campaign
	FROM #Web_Waterfall_Metrics_Data_Client
	  --FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] WITH (NOLOCK) 
	WHERE
	    utm_source = 'sms'
	 ;
	 CREATE INDEX #IX_SMS_SessionId ON #sms_campaign(sessionid);



	 DROP TABLE IF EXISTS #email_campaign
	 SELECT  DISTINCT SessionId 
	 INTO #email_campaign
	 FROM #Web_Waterfall_Metrics_Data_Client
	 --FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] WITH (NOLOCK) 
	where
	  (
	    utm_source = 'email'
	 )
	 OR
	 [Key]   IN  ('frictionless_Login' )
	 ;
	CREATE INDEX #IX_Email_SessionId ON #email_campaign(sessionid);



	 DROP TABLE IF EXISTS #Generic_campaign
	  SELECT  DISTINCT SessionId 
	 INTO #Generic_campaign
	 FROM #Web_Waterfall_Metrics_Data_Client
	 --FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] WITH (NOLOCK) 
	where
	  (
	  UTM_Campaign IS NULL 
	 AND  utm_source IS NULL
	 )
	 AND [Key]  NOT IN  ('frictionless_Login' )
	 ;
	CREATE INDEX #IX_Generic_SessionId ON #Generic_campaign(sessionid);
 

 	DROP TABLE IF EXISTS #Common_Email_SMS
	SELECT a.sessionid, CASE WHEN [key] = 'frictionless_login' THEN 'Email' ELSE UTM_Source END Campaign into #common_Email_SMS FROM #Web_Waterfall_Metrics_Data_Client a join
     (
     SELECT sessionid, max(capturedon) capturedon FROM #Web_Waterfall_Metrics_Data_Client where SessionId IN(
     SELECT s.SessionId FROM #sms_campaign s join #email_campaign e on s.SessionId = e.SessionId
     )
     and (UTM_Source IN ('Email', 'SMS') or [key] = 'frictionless_login') 
     group by SessionId 
     )b
     on a.SessionId = b.SessionId and a.CapturedOn = b.capturedon where a.UTM_Source is NOT NULL or [key] = 'frictionless_login'

	DELETE #Email_campaign from #Email_campaign e join
	 (SELECT Sessionid from #common_Email_SMS where campaign = 'SMS') s
	 on s.SessionId = e.Sessionid

	DELETE #sms_campaign from  #sms_campaign s join
	 (SELECT Sessionid from #common_Email_SMS where campaign = 'Email') e
	 on s.SessionId = e.Sessionid


	 DROP TABLE IF EXISTS #fl
	SELECT a.*, Campaign =  'Email', Campaign_Order = 1   
	INTO #fl
	  FROM #Web_Waterfall_Metrics_Data_Client a
	--FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] a WITH (NOLOCK) 
		JOIN #email_campaign on a.SessionId = #email_campaign.SessionId
	---where a.[key] IN ('checking_account_used','debit_card_used','credit_card_used')	


	UNION

	SELECT a.*, Campaign =  'SMS', Campaign_Order = 2  
	  FROM #Web_Waterfall_Metrics_Data_Client a
	--FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] a WITH (NOLOCK) 
		JOIN #sms_campaign on a.SessionId = #sms_campaign.SessionId
	---where a.[key] IN ('checking_account_used','debit_card_used','credit_card_used')

	UNION

	SELECT a.*, Campaign =  'Generic', Campaign_Order = 3   
	  FROM #Web_Waterfall_Metrics_Data_Client a
	--FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] a WITH (NOLOCK) 
	JOIN
		  ( SELECT #Generic_campaign.* FROM #Generic_campaign 
			LEFT JOIN 
			  (SELECT SessionId FROM #sms_campaign 
			  UNION 
			  SELECT SessionId 
			  FROM #email_campaign) sms_email_union ON #Generic_campaign.SessionId =  sms_email_union.SessionId
			  WHERE sms_email_union.sessionId IS NULL
		) Generic on a.SessionId = Generic.SessionId
	---where a.[key] IN ('checking_account_used','debit_card_used','credit_card_used')

;

/*
----------- There are few sessions where both Email and SMS campaigns are present.
----------- So, Need to update the Payment $ by dividing them by 2. 
----------- This will ensure the equal attribution to both Email and SMS campaigns as well as the removal of inflated Payment $ on aggregation.

UPDATE #fl 
set #fl.Payments = #fl.Payments/2 , #fl.Posted = #fl.Posted/2
 where #fl.metricid IN (
SELECT a.metricid from (
SELECT metricid, count(*) ct from #fl
where  
payments >0
and 
Campaign IN 
(
 'Email'
,'SMS'

)
group by metricid having count(*) >1
)a
)
and
payments >0
and Campaign IN 
(
 'Email'
,'SMS'
)
*/
-----------------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #t	
	SELECT 
	CAST(fl.CapturedOn AS DATE) CapturedDate
	,fl.KeySourceSystem
	,fl.ClientId
	, fl.Campaign
	  , COUNT(DISTINCT fl.SessionId) AS Sessions_or_Visits
	  , ISNULL(SUM(fl.Payments),0) AS Payments
	  , SUM(CASE WHEN dwk.Web_Key_Category = 'Login Succeed'  then 1 else 0 end) Logins
	  ,  SUM(CASE WHEN dwk.Web_Key_Category = 'Login Fail'  then 1 else 0 end) Logins_Failed
	  , SUM(CASE WHEN dwk.Web_Key_Category = 'Payment Succeed'  then 1 else 0 end) Payment_Succeed
	  , SUM(CASE WHEN dwk.Web_Key_Category = 'Payment Processing'  then 1 else 0 end) Payment_Processing
	  , fl.Campaign_Order
	 INTO #t
	  from #fl fl
		LEFT JOIN Analytics_2017.dbo.DimWebKey dwk WITH (NOLOCK) on dwk.web_key = fl.[Key]
	group by 
		CAST(fl.CapturedOn AS DATE)
		,fl.KeySourceSystem
		,fl.ClientId
		,fl.campaign
		,fl.Campaign_Order;


----------------------------------------INSERT INTO [CLIENT_ANALYTICS].[dbo].[Web_Waterfall_Visits_by_Source]-----------------------------------------------------------
	--TRUNCATE TABLE [CLIENT_ANALYTICS].[dbo].[Web_Waterfall_Visits_by_Source]
	INSERT INTO [CLIENT_ANALYTICS].[dbo].[Web_Waterfall_Visits_by_Source]
	(
	[CapturedDate]
		  ,[KeySourceSystem]
		  ,[ClientId]
		  ,[Campaign]
		  ,[Sessions_or_Visits]
		  ,[Payments]
		  ,[Logins]
		  ,[Logins_Failed]
		  ,[Payment_Succeed]
		  ,[Payment_Processing]
		  ,[Campaign_Order]
	)
	SELECT  [CapturedDate]
		  ,[KeySourceSystem]
		  ,[ClientId]
		  ,[Campaign]
		  ,[Sessions_or_Visits]
		  ,[Payments]
		  ,[Logins]
		  ,[Logins_Failed]
		  ,[Payment_Succeed]
		  ,[Payment_Processing]
		  ,[Campaign_Order]
	  FROM #t
	;




END;