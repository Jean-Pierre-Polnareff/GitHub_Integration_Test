USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[sp_insert_RPT_SMS_Optout_Excpt]    Script Date: 5/3/2023 9:52:38 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 
-- =============================================
-- Object: dbo.[sp_insert_RPT_SMS_Optout_Excpt]
-- Create date: 06/23/2022
--
-- Description: Identify and insert Email OptOut exceptions into RPT_SMS_Optout_Excpt_CRM_level_Count and RPT_SMS_Optout_Excpt_Customer_level
--
-- History
-- Author Date Description
-- ------------------------------------------------------
-- Parth Dave 06/23/2022 Created 
-- Vlad Pilipets 2023-05-02 enhancing sms optout logic 
-- * disregard first post stop delivery 
-- * check for the same phone 
-- * disregard inbound messages
-- * 1 hour for the same phone number, post stop, 
-- =============================================

ALTER PROCEDURE [dbo].[sp_insert_RPT_SMS_Optout_Excpt]

 @startdatetime datetime = NULL,

 @end datetime =  NULL

AS

SET NOCOUNT ON;

---To handle logic for Amex first party flag lookup table

TRUNCATE TABLE DW_STAGING.dbo.Amex_ClientCodes_LookupTable;

INSERT DW_STAGING.dbo.Amex_ClientCodes_LookupTable(ClientCode,StandardParentCode,[Description],FirstPartyFlag,amex_segment,Amex_Segment_Description)
SELECT DISTINCT ClientCode,StandardParentCode,[Description],FirstPartyFlag,amex_segment,Amex_Segment_Description
FROM OpenQuery([HVDB02.CORPGLBDOM.LOCAL],'SET NOCOUNT ON;
	  SELECT ClientCode
			 , StandardParentCode
			 , Description
			 , FirstPartyFlag
			 , CASE WHEN description LIKE ''%legal%'' AND firstpartyflag=1 THEN ''Amex - Legal''
			        WHEN description NOT LIKE ''%legal%'' AND firstpartyflag=1 THEN ''Amex - Firstparty Non-legal''
					WHEN firstpartyflag=0 THEN ''Amex - 3rd Party''
					ELSE ''Amex - Other''
					END AS amex_segment
			 , Description AS Amex_Segment_Description 
	  FROM Amex.dbo.Amex_ClientCodes_LookupTable with (NoLock)');

DROP TABLE IF EXISTS #vw_Amex_ClientCodes_LookupTable

SELECT l.*,s.Amex_Segment_Group
INTO #vw_Amex_ClientCodes_LookupTable
	  FROM DW_STAGING.dbo.Amex_ClientCodes_LookupTable l
	          left JOIN
           CLIENT_ANALYTICS.dbo.Amex_Segment_Group s (NOLOCK) ON l.clientcode=s.ClientId

CREATE INDEX i_ClientCode ON #vw_Amex_ClientCodes_LookupTable(ClientCode ASC);


-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_Excpt_CRM_level_Count for Yesterday------------------------------------------------------------------

DELETE FROM  CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_Excpt_CRM_level_Count
WHERE SMS_Sent_Date = cast(isnull(@end, DATEADD(day, DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
	AND CAST([Insert_Date] AS DATE) =  CAST(GETDATE() AS DATE)

--------cartesian for yesterday------

IF OBJECT_ID('tempdb..#t') IS NOT NULL
	  	DROP TABLE #t;

select  isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as day
, dss.KeySourceSystem 
, dde.Digital_excpt_id
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
	CROSS JOIN CLIENT_ANALYTICS.dbo.dim_digital_sms_excpt dde
WHERE dss.KeySourceSystem not in (4, 0)

----sent SMS data with Livevox result SMS MT Delivered','SMS MT Failed for exception rule_1----

DROP TABLE IF EXISTS #temp_sms

SELECT fct.KeyCustomer  
		, fct.CallStartTime  
		, fct.DialedPhoneNumber
		, fct.SessionID 
		, fct.KeySourceSystem  
		, fct.KeyDate_CallDate 
		, rc.Livevox_Result 
		, rc.Answer_Type 
		, rc.Service_Name 
		, dt.WeekId 
	INTO #temp_sms 
	FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
		join DW_MSTR_DM.dbo.RadiusCall rc (NOLOCK) ON fct.SessionId = rc.Session_Id  
														AND rc.Call_Date >= ISNULL(@startdatetime, GETDATE()) - 31     
		join DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
	WHERE (rc.livevox_result like 'SMS%'  
			OR rc.livevox_result like '%Text%')
	      --and cast(rc.question_9 as int) > 0			 				--per TedM 6/29, templateid>0 indicates valid send
          AND (rc.Call_Date >= ISNULL(@startdatetime, GETDATE()) - 31 
				AND rc.Call_Date >= '2022-06-22')
		  AND dt.CalendarDate >= ISNULL(@startdatetime, GETDATE()) - 31

DROP TABLE IF EXISTS #SMS 
 
SELECT t.KeyCustomer  
		, t.CallStartTime  
		, t.DialedPhoneNumber
		, t.SessionID 
		, t.KeySourceSystem 
		, t.KeyDate_CallDate 
		, t.Livevox_Result 
		, t.Service_Name 
		, t.WeekId 
		, cust.ClientId   
		, cust.CustomerId 
		, dss.SourceSystem 
		, dcl.ClientParent
		, rp.SMS_optout_date
		-----to find previous 'SMS sent (SMS MT delivered + SMS Failed')' 
		, LAG(t.CallStartTime,1) OVER (PARTITION BY cust.KeyCustomer, t.DialedPhoneNumber ORDER BY t.CallStartTime) AS previous_callstarttime
		-----to find previous 'SMS Delivered'
		, CASE WHEN (t.Livevox_Result like 'SMS%' AND t.Livevox_Result like '%Sent%' AND t.Livevox_Result like '%Authenticated%' AND Answer_Type like 'Inbound%' AND Answer_Type like '%Right Party%') THEN 1 ELSE 0 END as IsCurrentInbound  
		, LAG(CASE WHEN t.Livevox_Result = 'SMS MT Delivered' THEN 1 ELSE 0 END,1) OVER (PARTITION BY cust.KeyCustomer, t.DialedPhoneNumber ORDER BY t.CallStartTime) AS IsPreviousDelivered  
		, LAG(CASE WHEN (t.Livevox_Result like 'SMS%' AND t.Livevox_Result like '%Sent%' AND t.Livevox_Result like '%Authenticated%' AND Answer_Type like 'Inbound%' AND Answer_Type like '%Right Party%') THEN 1 ELSE 0 END, 1) OVER (PARTITION BY cust.KeyCustomer, t.DialedPhoneNumber ORDER BY t.CallStartTime) as IsPreviousInbound   
		, LAG(CASE WHEN t.Livevox_Result like '%Stop%' THEN 1 ELSE 0 END,1) OVER (PARTITION BY cust.KeyCustomer, t.DialedPhoneNumber ORDER BY t.CallStartTime) AS IsPreviousStop  
		, LAG(CASE WHEN t.Livevox_Result like '%Stop%' THEN 1 ELSE 0 END,2) OVER (PARTITION BY cust.KeyCustomer, t.DialedPhoneNumber ORDER BY t.CallStartTime) AS IsPostStopSent 
INTO #SMS 
FROM #temp_sms t 
	JOIN DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on t.KeySourceSystem = dss.KeySourceSystem
	JOIN DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK) ON t.KeyCustomer = cust.KeyCustomer  
	JOIN DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer  
											and CAST(rp.PhoneNumber AS VARCHAR) = CAST(t.DialedPhoneNumber AS VARCHAR) 
	LEFT JOIN DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId  
											and cust.SourceSystem=dcl.SourceSystem
	LEFT JOIN #vw_Amex_ClientCodes_LookupTable (nolock) lup on lup.ClientCode= cust.ClientId     
											and cust.SourceSystem = 'AMEX Latitude'
WHERE (lup.FirstPartyFlag <> 1  or lup.FirstPartyFlag is NULL)

------------SMS_optout exceptions - SMS sent after consumer requested to stop to text------

	DROP TABLE IF EXISTS #exceptions

	SELECT   Digital_excpt_id = 1
	       , s.ClientId
		   , s.KeyCustomer
		   , s.CustomerId
		   , s.KeySourceSystem
		   , s.SourceSystem
		   , s.CallStartTime
		   , previous_callstarttime = null 
		   , date_difference = null 
		   , s.WeekId
		   , s.DialedPhoneNumber
		   , s.SessionId  
		   , s.ClientParent
		   , s.SMS_optout_date
		   , stoptotext_date = null 
		   , ispoststopsent [SMS_Stopped_flag]

	INTO #exceptions
	FROM #SMS s
	WHERE ispoststopsent = 1 
		AND iscurrentinbound = 0 
		AND ispreviousinbound = 0  
		AND ispreviousdelivered = 1 
		AND livevox_result NOT LIKE '%Stop%'  
		AND DATEDIFF(MINUTE,previous_callstarttime, CallStartTime) >  60 
			----9/20/22 As per TedM, excluding Bob's customerid for thirdprod
			AND NOT (s.CustomerId = 102231 AND s.KeySourceSystem = 2)			
			---10/3 As per TedM, excluding Test customer IDs for Artiva and HTI customer
			AND NOT (s.CustomerId in ('11644','1224851','1687','1028999','14892599') and s.KeySourceSystem in (1,2,3)) 

	union
	
--------Exceptions SMS sent to accounts within 2 days---------
	
	select   Digital_excpt_id = 2
           , s.ClientId
		   , s.KeyCustomer
		   , s.CustomerId
		   , s.KeySourceSystem
		   , s.SourceSystem
		   , s.CallStartTime 
		   , s.previous_callstarttime 
		   , date_difference = datediff(day,s.previous_callstarttime, s.CallStartTime)           
		   , s.WeekId
		   , s.DialedPhoneNumber
		   , s.SessionId
		   , s.ClientParent
		   , s.SMS_optout_date
		   , Stoptotext_date = NULL     
		   , [SMS_Stopped_flag] = NULL	   		   		   	
	from #SMS s
	where (DATEDIFF(day,s.previous_callstarttime, s.CallStartTime) between 1 and 2)
		and s.IsPreviousDelivered = 1 
		and s.IsPreviousStop = 0 
		and s.IsPostStopSent = 0 
		and s.Livevox_Result = 'SMS MT Delivered'
		and not (s.CustomerId = 102231 and s.KeySourceSystem = 2)      ----9/20/22 As per TedM, excluding Bob's customerid for thirdprod
		---10/3 As per TedM, excluding Test customer IDs for Artiva and HTI customer
		and not (s.CustomerId in ('11644','1224851','1687','1028999','14892599') and s.KeySourceSystem in (1,2,3)) 
		---12/20/22 per TedM, no more 1 SMS in 3 day rule after 12/19/22
		and cast(s.CallStartTime as date)<'12/19/22'
		and (
			 cast(s.CallStartTime as date)<'11/17/22'
			 or (
				  s.Service_Name not like '%jacksonville_oliphant%'
				  and s.Service_Name not like '%dec_3p-jfc%'
				  and s.Service_Name not like '%ncb_seconds%'
				  and s.Service_Name not like '%mum_bureaus%'
				  and s.Service_Name not like '%jax_jeffcap%'
				  and s.Service_Name not like '%dec_3p-eversource%'
				  and s.Service_Name not like '%pendrick3%'
				  and s.Service_Name not like '%pendrick2%'
				  and s.Service_Name not like '%dec_3p-fpb%'
				  and s.Service_Name not like '%uverse%'
				  and s.Service_Name not like '%DEC_3P-Cox%'
				  and s.Service_Name not like '%mbj_FPB%'
				)
			)

	union
	
--------Exceptions SMS sent to accounts within 7 days - list of services eligible---------
	
	select   Digital_excpt_id = 3 
           , s.ClientId
		   , s.KeyCustomer
		   , s.CustomerId
		   , s.KeySourceSystem
		   , s.SourceSystem
		   , s.CallStartTime 
		   , s.previous_callstarttime 
		   , date_difference = datediff(day,s.previous_callstarttime, s.CallStartTime)           
		   , s.WeekId
		   , s.DialedPhoneNumber
		   , s.SessionId 
		   , s.ClientParent
		   , s.SMS_optout_date
		   , Stoptotext_date = NULL     
		   , [SMS_Stopped_flag] = NULL	   		   		   	
	from #SMS s 
	where (DATEDIFF(day,s.previous_callstarttime, s.CallStartTime) <= 6 
		and DATEDIFF(day,s.previous_callstarttime , s.CallStartTime) > 0)
		and s.IsPreviousDelivered = 1 
		and s.IsPreviousStop = 0 
		and s.IsPostStopSent = 0 
		and s.Livevox_Result = 'SMS MT Delivered' 
		and not(s.CustomerId = 102231 and s.KeySourceSystem = 2)      ----9/20/22 As per TedM, excluding Bob's customerid for thirdprod
	 ---10/3 As per TedM, excluding Test customer IDs for Artiva and HTI customer
		and not (s.CustomerId in ('11644','1224851','1687','1028999','14892599') and s.KeySourceSystem in (1,2,3)) 
	 ---12/20/22 per TedM, after 12/18/22, all clients are 1 SMS in 7
		and (
			 (
			 cast(s.CallStartTime as date) between '11/17/22' and '12/18/22'
			 and (
				  s.Service_Name like '%jacksonville_oliphant%'
				  or s.Service_Name like '%dec_3p-jfc%'
				  or s.Service_Name like '%ncb_seconds%'
				  or s.Service_Name like '%mum_bureaus%'
				  or s.Service_Name like '%jax_jeffcap%'
				  or s.Service_Name like '%dec_3p-eversource%'
				  or s.Service_Name like '%pendrick3%'
				  or s.Service_Name like '%pendrick2%'
				  or s.Service_Name like '%dec_3p-fpb%'
				  or s.Service_Name like '%uverse%'
				  or s.Service_Name like '%DEC_3P-Cox%'
				  or s.Service_Name like '%mbj_FPB%'
				)
			  )
			  or
			 cast(s.CallStartTime as date)>='12/19/22'
			)
 
		      
	order by s.CallStartTime


--------------group by CRM to calculate count of exceptions-------

drop table if exists #groupby_crm

select 
         Digital_excpt_id
       , cast(CallStartTime as date) SMS_Sent_Date 
	   , KeySourceSystem
       , SourceSystem
	   , count(*) as count_exceptions

into #groupby_crm

	   from #exceptions exc
	   group by Digital_excpt_id, cast(CallStartTime as date), SourceSystem, KeySourceSystem


-----SMS Sent for yesterday-------

drop table if exists #sms_sent_for_yesterday

select cast(CallStartTime as date) as SMS_Sent_Date, KeySourceSystem,  count(*) sms_sent
into #sms_sent_for_yesterday
from 
#SMS 
where cast(CallStartTime as date)= cast(isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
group by cast(CallStartTime as date), KeySourceSystem


------------Insert SMS optout excpetions in CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_Excpt_CRM_level_Count------
 
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_Excpt_CRM_level_Count
( 
	Digital_excpt_id
	,SMS_Sent_Date
	,KeySourceSystem
	,Count_of_Exceptions
	,Insert_Date
) 
SELECT Digital_excpt_id, SMS_Sent_Date, KeySourceSystem, Count_of_Exceptions, Insert_Date
FROM 
	 (
		SELECT CAST(#t.day AS DATE) SMS_Sent_Date
		   ,#t.KeySourceSystem
		   ,#t.Digital_excpt_id
		   ,ISNULL(SUM(x.count_exceptions),0) AS Count_of_Exceptions
		   ,GETDATE() AS Insert_Date
		   ,y.sms_sent
		FROM #t
			LEFT JOIN #groupby_crm x on CAST(x.SMS_Sent_Date AS DATE) =  CAST(#t.day AS DATE)
								AND x.KeySourceSystem = #t.KeySourceSystem
								AND x.Digital_excpt_id = #t.Digital_excpt_id
			LEFT JOIN #sms_sent_for_yesterday y ON cast(y.SMS_Sent_Date AS DATE) =  CAST(#t.day AS DATE)
								AND #t.KeySourceSystem = y.KeySourceSystem
		GROUP BY #t.Digital_excpt_id, CAST(#t.day AS DATE), #t.KeySourceSystem, y.sms_sent
	) abc
WHERE ISNULL(abc.sms_sent,0) > 0 
 
------------insert SMS optout excpetions client_analytics.dbo.RPT_SMS_Optout_ Excpt_Customer_level------

INSERT INTO client_analytics.dbo.RPT_SMS_Optout_Excpt_Customer_level
(
         Digital_excpt_id
       , ClientId
	   , KeyCustomer
	   , CustomerId
	   , KeySourceSystem
	   , SourceSystem
 	   , CallDate
	   , CallStartTime 
	   , previous_callstarttime
	   , date_difference
	   , WeekId
	   , DialedPhoneNumber
	   , SessionId
	   , ClientParent
 	   , SMS_optout_date
	   , Stoptotext_date
       , [SMS_Stopped_flag]
	   , insert_date
)

SELECT   exc.Digital_excpt_id
       , exc.ClientId
	   , exc.KeyCustomer
	   , exc.CustomerId
	   , exc.KeySourceSystem
	   , exc.SourceSystem
 	   , cast(exc.CallStartTime as date) as CallDate
	   , exc.CallStartTime 
	   , exc.previous_callstarttime
	   , exc.date_difference
	   , exc.WeekId
	   , exc.DialedPhoneNumber
	   , exc.SessionId
	   , exc.ClientParent
 	   , exc.SMS_optout_date
	   , exc.Stoptotext_date
       , exc.[SMS_Stopped_flag]
	   , insert_date = getdate()
FROM #exceptions exc
	LEFT JOIN CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_Excpt_Customer_level ecl (NOLOCK) ON exc.SessionId = ecl.SessionId
WHERE ecl.SessionId IS NULL

go  

