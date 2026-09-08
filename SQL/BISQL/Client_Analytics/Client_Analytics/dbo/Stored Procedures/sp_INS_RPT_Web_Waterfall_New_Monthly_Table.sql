USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[sp_INS_RPT_Web_Waterfall_New_Monthly_Table]    Script Date: 5/22/2025 10:55:58 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




/*
-- =============================================
-- Object: dbo.sp_INS_RPT_Web_Waterfall_New_Monthly_Table
-- Create date: 12/28/2023
--
--  Description: Inserts data for New Monthly summary from [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] into [Client_Analytics].[dbo].[RPT_Web_Waterfall_New_Monthly_Table]
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	12/28/2023	 Created
-- =============================================
*/

CREATE PROCEDURE [dbo].[sp_INS_RPT_Web_Waterfall_New_Monthly_Table] 
	@startdate DATE = NULL

AS

BEGIN
	SET NOCOUNT ON;

	SET @startdate = ISNULL(@startdate,DATEADD(dd,-60,CAST(GETDATE() AS DATE))); 

	DELETE FROM CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table
	WHERE CAST(CapturedOn AS DATE) >= @startdate
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
	WHERE  utm_source = 'sms'
	;
CREATE INDEX #IX_SMS_SessionId ON #sms_campaign(sessionid);

	DROP TABLE IF EXISTS #email_campaign
	SELECT  DISTINCT SessionId 
	INTO #email_campaign
	FROM #Web_Waterfall_Metrics_Data_Client
	WHERE
	(
		  utm_source = 'email'
	)
		OR [Key]   IN  ('frictionless_Login' )
	;
	CREATE INDEX #IX_Email_SessionId ON #email_campaign(sessionid);

	DROP TABLE IF EXISTS #unspecified_campaign
	SELECT  DISTINCT SessionId 
	INTO #unspecified_campaign
	FROM #Web_Waterfall_Metrics_Data_Client
	WHERE
		(
			UTM_Campaign IS NULL 
			AND  utm_source IS NULL
		)
			AND [Key]  NOT IN  ('frictionless_Login' )
	;
	CREATE INDEX #IX_Unspecified_SessionId ON #unspecified_campaign(sessionid);

----------- There are few sessions where both Email and SMS campaigns are present.
----------- So,Channel is allocated to latest present in that SessionID(SMS/Email). 

	 DROP TABLE IF EXISTS #Common_Email_SMS

	 SELECT a.sessionid, CASE WHEN [key] = 'frictionless_login' THEN 'Email' ELSE UTM_Source END Campaign INTO #common_Email_SMS FROM #Web_Waterfall_Metrics_Data_Client a join
	(
		SELECT sessionid, MAX(capturedon) capturedon FROM #Web_Waterfall_Metrics_Data_Client WHERE SessionId IN(
		SELECT s.SessionId FROM #sms_campaign s join #email_campaign e ON s.SessionId = e.SessionId
	)
		AND (UTM_Source IN ('Email', 'SMS') or [key] = 'frictionless_login') 
		GROUP BY SessionId 
	)b
		ON a.SessionId = b.SessionId AND a.CapturedOn = b.capturedon 
		WHERE a.UTM_Source IS NOT NULL OR [key] = 'frictionless_login'

	DELETE #sms_campaign 
	FROM  #sms_campaign s 
	JOIN
	 (SELECT Sessionid FROM #common_Email_SMS WHERE campaign = 'Email') e
	 ON s.SessionId = e.Sessionid

	DELETE #Email_campaign 
	FROM #Email_campaign e 
	JOIN
	 (SELECT Sessionid FROM #common_Email_SMS WHERE campaign = 'SMS') s
	 ON s.SessionId = e.Sessionid

	DROP TABLE IF EXISTS #t
	SELECT a.*, Campaign =  'Email', Campaign_Order = 1   
	INTO #t
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN #email_campaign
		ON a.SessionId = #email_campaign.SessionId

	UNION

	SELECT a.*, Campaign =  'SMS', Campaign_Order = 2  
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN #sms_campaign
		ON a.SessionId = #sms_campaign.SessionId

	UNION

	SELECT a.*, Campaign =  'SSN/DoB Failed', Campaign_Order = 3   
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId 
			FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [Key]   IN ('ssn_login_failed', 'dob_login_failed')
		) SSN_or_DoB_Failed
			ON a.SessionId = SSN_or_DoB_Failed.SessionId

	UNION

	SELECT a.*, Campaign =  'SSN/DoB Succeeded', Campaign_Order = 4   
	FROM #Web_Waterfall_Metrics_Data_Client a
		JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [Key]   IN ('ssn_login_Succeeded', 'dob_login_Succeeded')
		) SSN_or_DoB_Succeeded
			ON a.SessionId = SSN_or_DoB_Succeeded.SessionId

	UNION

	SELECT a.*, Campaign =  'Future Payments Set Up', Campaign_Order = 5   
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [PaySeriesCount] IS NOT NULL
		) Future_Payments_Set_Up
			ON a.SessionId = Future_Payments_Set_Up.SessionId

	UNION

	SELECT a.*, Campaign =  'Future Payment $ Set Up', Campaign_Order = 6   
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [PaySeriesAmount] IS NOT NULL
		) Future_Payment_$_Set_Up
			ON a.SessionId = Future_Payment_$_Set_Up.SessionId

	UNION

	SELECT a.*, Campaign =  'AXP Payment Programs', Campaign_Order = 7   
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = 'program_enrollment'
				AND SessionId IS NOT NULL
		) AXP_Payment_Programs
			ON a.SessionId = AXP_Payment_Programs.SessionId

	UNION

	SELECT a.*, Campaign =  'pay_in_full_submitted', Campaign_Order = 8   
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE[key] = 'pay_in_full_submitted'
				AND SessionId IS NOT NULL
		) pay_in_full_submitted
			ON a.SessionId = pay_in_full_submitted.SessionId
	
	UNION

	SELECT a.*, Campaign =  'installment_submitted', Campaign_Order = 9   
	FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = 'installment_submitted'
				AND SessionId IS NOT NULL
		) installment_submitted
			ON a.SessionId = installment_submitted.SessionId

	UNION

	SELECT a.*, Campaign =  'settlement_submitted', Campaign_Order = 10   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = 'settlement_submitted'
				AND SessionId IS NOT NULL
		) settlement_submitted
			ON a.SessionId = settlement_submitted.SessionId

	UNION

	SELECT a.*, Campaign =  'one_time_payment_submitted', Campaign_Order = 11   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = 'one_time_payment_submitted'
				AND SessionId IS NOT NULL
		) one_time_payment_submitted
			ON a.SessionId = one_time_payment_submitted.SessionId

	UNION

	SELECT a.*, Campaign =  'reocurring_submitted', Campaign_Order = 12   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = 'reocurring_submitted'
				AND SessionId IS NOT NULL
		) reocurring_submitted
			ON a.SessionId = reocurring_submitted.SessionId

	UNION

	SELECT a.*, Campaign =  'Unspecified', Campaign_Order = 13   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		( SELECT #unspecified_campaign.* FROM #unspecified_campaign 
			LEFT JOIN 
			(SELECT SessionId FROM #sms_campaign 
			UNION 
			SELECT SessionId FROM #email_campaign) sms_email_union
				ON #unspecified_campaign.SessionId =  sms_email_union.SessionId
			WHERE sms_email_union.sessionId IS NULL
		) Unspecified
			ON a.SessionId = Unspecified.SessionId
	
	UNION

	SELECT a.*, Campaign =  'PIN', Campaign_Order = 14   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] IN ( 'pin_login_succeeded', 'pin_login_failed')
				AND SessionId IS NOT NULL
		) pin_login
			ON a.SessionId = pin_login.SessionId

	UNION

	SELECT a.*, Campaign =  'Frictionless', Campaign_Order = 15   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = ( 'frictionless_login')
				AND SessionId IS NOT NULL
		) frictionless_login
			ON a.SessionId = frictionless_login.SessionId

	UNION

	SELECT a.*, Campaign =  'Primary', Campaign_Order = 16   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = ( 'prim_login_succeeded')
				AND SessionId IS NOT NULL
		) prim_login_succeeded
			ON a.SessionId = prim_login_succeeded.SessionId

	UNION

	SELECT a.*, Campaign =  'Comaker', Campaign_Order = 17   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = ( 'comaker_login_succeeded')
				AND SessionId IS NOT NULL
		) comaker_login_succeeded
			ON a.SessionId = comaker_login_succeeded.SessionId

	UNION

	SELECT a.*, Campaign =  'Checks', Campaign_Order = 18   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = ( 'checking_account_used')
				AND SessionId IS NOT NULL
		) checking_account_used
			ON a.SessionId = checking_account_used.SessionId


	UNION

	SELECT a.*, Campaign =  'Debit Cards', Campaign_Order = 19   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = ( 'debit_card_used')
				AND SessionId IS NOT NULL
		) debit_card_used 
			ON a.SessionId =debit_card_used.SessionId

	UNION

	SELECT a.*, Campaign =  'Credit Cards', Campaign_Order = 20   FROM #Web_Waterfall_Metrics_Data_Client a
	JOIN
		(
			SELECT  DISTINCT SessionId FROM #Web_Waterfall_Metrics_Data_Client
			WHERE [key] = ( 'credit_card_used')
				AND SessionId IS NOT NULL
		) credit_card_used 
			ON a.SessionId =credit_card_used.SessionId
	;
    
/*
----------- There are few sessions where both Email and SMS campaigns are present.
----------- So, Need to update the Payment $ by dividing them by 2. 
----------- This will ensure the equal attribution to both Email and SMS campaigns as well as the removal of inflated Payment $ on aggregation.
UPDATE #t 
set #t.Payments = #t.Payments/2 , #t.Posted = #t.Posted/2
 where #t.metricid IN (
SELECT a.metricid from (
SELECT metricid, count(*) ct from #t
where  payments >0
and Campaign IN 
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


----------------------------------------------INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table-----------------------------------------------------
	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_New_Monthly_Table
	(
	   [MetricId]
      ,[Key]
      ,[CapturedOn]
      ,[Count]
      ,[Description]
      ,[ReferenceNumber]
      ,[Payments]
      ,[ClientId]
      ,[ClientParent]
      ,[PDC]
      ,[Posted]
      ,[IpAddress]
      ,[SessionId]
      ,[QueryString]
      ,[UTM_Source]
      ,[UTM_Campaign]
      ,[SourceSystem]
      ,[KeySourceSystem]
      ,[KeyCustomer]
      ,[KeyClient]
      ,[CustomerID]
      ,[KeyETLAuditHistory_Inserted]
      ,[KeyETLAuditHistory_Last_Updated]
      ,[InsertDate]
      ,[UpdateDate]
      ,[OneTimePaymentDate]
      ,[OneTimePaymentAmount]
      ,[PaySeriesStart]
      ,[PaySeriesFrequency]
      ,[PaySeriesAmount]
      ,[PaySeriesCount]
      ,[PaySumTotal]
      ,[DeviceCategory]
      ,[OS]
      ,[Browser]
      ,[Campaign]
      ,[Campaign_Order]
	  )
	 SELECT 
	   [MetricId]
      ,[Key]
      ,[CapturedOn]
      ,[Count]
      ,[Description]
      ,[ReferenceNumber]
      ,[Payments]
      ,[ClientId]
      ,[ClientParent]
      ,[PDC]
      ,[Posted]
      ,[IpAddress]
      ,[SessionId]
      ,[QueryString]
      ,[UTM_Source]
      ,[UTM_Campaign]
      ,[SourceSystem]
      ,[KeySourceSystem]
      ,[KeyCustomer]
      ,[KeyClient]
      ,[CustomerID]
      ,[KeyETLAuditHistory_Inserted]
      ,[KeyETLAuditHistory_Last_Updated]
      ,[InsertDate]
      ,[UpdateDate]
      ,[OneTimePaymentDate]
      ,[OneTimePaymentAmount]
      ,[PaySeriesStart]
      ,[PaySeriesFrequency]
      ,[PaySeriesAmount]
      ,[PaySeriesCount]
      ,[PaySumTotal]
      ,[DeviceCategory]
      ,[OS]
      ,[Browser]
      ,[Campaign]
      ,[Campaign_Order] 
	FROM #t
	;

END;


GO