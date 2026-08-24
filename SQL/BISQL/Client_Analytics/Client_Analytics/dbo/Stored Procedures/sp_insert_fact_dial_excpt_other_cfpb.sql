USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_cfpb_other]    Script Date: 2/15/2022 5:49:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






alter PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb_other]
	
	@StartDateTime DATETIME = NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb_other

Description: Identify and insert dialer exceptions for CFPB new rules into fact_dial_excpt_other (non-DW CRMs)

Author			Date		Description
Mike Campbell	05/03/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_other_CRM_level_count]
WHERE calldate = @end
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id IN (9,10) 


-------------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT @end AS day
       , dde.dlr_expt_id 
INTO #t 
FROM CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
WHERE dde.dlr_expt_id IN (9,10) 
AND dde.all_client_flag = 1
------------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------


	
	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	SELECT 
			NULL AS KeyCustomerCall
			, rc.Creditor_Code AS clientid
			, NULL AS KeyCustomer
			, CAST(SUBSTRING(rc.Account_Number,5,9) AS INT) AS CustomerId
			, rc.Agent_Logon_Id
			, NULL AS KeySourceSystem
			, NULL AS SourceSystem
			, rc.Call_Connect_Time_CT AS CallStartTime
			, NULL AS WeekId
			, rc.Phone_Dialed AS DialedPhoneNumber
			, LEFT(rc.Phone_Dialed,3) AS DialedAreaCode
			, rc.Session_Id AS SessionId
			, CASE when rc.Is_RPC=1 then 1 else 0 end as contact_flag
			, rc.Agent_Logon_Id AS EmployeeId
			, rc.Call_Duration AS CallSeconds
			, NULL AS ClientParent
			, rc.Is_RPC AS IsRPC
--			, rc.Livevox_Result
	--7th previous call start time since limit of 7 calls in 7 days.  If 7th call in less than 7 days, exception.
			, LAG(rc.Call_Connect_Time_CT,7) OVER(PARTITION BY rc.Account_Number ORDER BY rc.Call_Connect_Time_CT) AS callstarttime_lag7
	--isrpc_lag1 indicates previous call was an RPC
			, LAG(rc.Is_RPC,1) OVER(PARTITION BY rc.Account_Number ORDER BY rc.Call_Connect_Time_CT) AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG(rc.Call_Connect_Time_CT,1) OVER(PARTITION BY rc.Account_Number ORDER BY rc.Call_Connect_Time_CT) AS callstarttime_lag1
	INTO #calls 
	FROM DW_MSTR_DM.dbo.RadiusCall rc (nolock) 
	WHERE rc.Call_Date > isnull(@StartDateTime,GETDATE())-31
	      AND rc.Transaction_Type='Outbound'
		  AND (CASE WHEN LEFT(rc.Phone_Dialed,3)='800' AND CAST(SUBSTRING(rc.Account_Number,5,9) AS INT)=0 THEN 1 ELSE 0 END) = 0
          AND rc.Call_Date>='11/30/21'
		  AND LEFT(rc.Account_Number,4)='PROD'
		  AND rc.LV_Client_Name='RGS-CCS'
		  AND rc.Platform_Id='PROD'
		  AND rc.Livevox_Result NOT IN('AGENT - Busy','Busy','Invalid Phone Number');


	--EXCEPTIONS
	--identify exceptions for each rule

	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlr_expt_id=9:  CFPB - 7 attempts per 7 sliding
		SELECT 9 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.KeySourceSystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
		INTO #exceptions
		FROM #calls c
		WHERE DATEDIFF(DAY,c.callstarttime_lag7,c.CallStartTime)<7
		UNION
		--dlr_expt_id=10:  CFPB - Post-RPC 0 attempts 7 days
		SELECT 10 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.KeySourceSystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
		FROM #calls c
		WHERE isrpc_lag1=1
	          AND DATEDIFF(DAY,c.callstarttime_lag1,c.CallStartTime)<7


--------------------------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count] for Yesterday----------------------------

INSERT INTO  CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_other_CRM_level_count]
SELECT calldate
, NULL AS keysourcesystem
,dlr_expt_id
,Count_of_Exceptions
,Insert_Date
FROM
(
SELECT #t.day AS calldate
, null AS keysourcesystem
,#t.dlr_expt_id AS dlr_expt_id
,ISNULL(SUM(E.Count_of_Exceptions),0) AS Count_of_Exceptions
,GETDATE() AS Insert_Date
,c.No_of_Calls
FROM #t
LEFT JOIN 
(SELECT CAST(exc.CallStartTime AS DATE) calldate
,NULL AS keysourcesystem
,exc.dlr_excpt_id
,COUNT(*) AS Count_of_Exceptions
FROM #exceptions exc 
GROUP BY CAST(exc.CallStartTime AS DATE)
,exc.dlr_excpt_id
) E
ON CAST(e.calldate AS DATE) = #t.day
	AND e.dlr_excpt_id = #t.dlr_expt_id
LEFT JOIN 
(
SELECT 
fct.Call_Date AS calldate
, NULL AS KeySourceSystem
, COUNT(*) AS No_of_Calls 
FROM  DW_MSTR_DM.dbo.RadiusCall fct (NOLOCK)
WHERE fct.Call_Date  = CAST(@end AS DATE)
      AND fct.platform_id='PROD'
GROUP BY fct.Call_Date
)c
ON #t.day = CAST(c.calldate AS DATE) 
GROUP BY 
#t.day 
,#t.dlr_expt_id   
,c.No_of_Calls
)f
WHERE ISNULL(f.No_of_Calls,0) > 0



--------------------------------------------------Insert to fact_dial_excpt--------------------------------------------------
	INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt_other(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,
	                                               clientid,sourcesystem,calldate,sessionid,insert_date,
	                                               DialedPhoneNumber,CallStartTime,EmployeeID,CallSeconds,ClientParent)
	SELECT exc.dlr_excpt_id
		   , exc.keycustomercall
		   , exc.call_history_fact_id
		   , exc.customerid
		   , exc.clientid
		   , exc.sourcesystem
		   , exc.calldate
		   , exc.sessionid
		   , exc.insert_date
		   , exc.DialedPhoneNumber
		   , exc.CallStartTime
		   , exc.EmployeeID
		   , exc.CallSeconds
		   , exc.ClientParent
	FROM #exceptions exc
			LEFT OUTER JOIN
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt_other fde (NOLOCK) ON exc.dlr_excpt_id=fde.dlr_excpt_id
		                                                            AND exc.CustomerId=fde.customerid
		                                                            AND exc.CallStartTime=fde.CallStartTime
	WHERE fde.customerid IS NULL 
	ORDER BY exc.calldate

END;









--GO
