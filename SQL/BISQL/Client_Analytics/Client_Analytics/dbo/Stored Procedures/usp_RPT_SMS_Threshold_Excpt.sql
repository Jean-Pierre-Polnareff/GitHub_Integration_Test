USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[usp_RPT_Envision_Placement]    Script Date: 6/13/2023 8:15:37 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[usp_RPT_SMS_Threshold_Excpt]
AS 
BEGIN

SET NOCOUNT ON; 

DECLARE @max_date DATETIME 
SELECT @max_date = MAX(Insert_Date)   
FROM CLIENT_ANALYTICS.dbo.RPT_SMS_Threshold_Excpt 

/*
	1. Rules are driven by DimAttemptThresholds table
	* attempt limit
	* interval 
	* months 
	2. collect all sms messages with 
	* rank & row - it will calculate number of attempts 
	* interval between the calls
	3. Exceptions: 
	* attempt number is above maxed out #1 attampt limit 
	* interval is above #1 interval 
*/ 
		
/*
	collect all sms messages 
*/
drop table if exists #sms_raw 

SELECT fcc.keycustomer
		, fcc.keysourcesystem
		, rc.Call_Date as sms_send_date  
		, rc.Service_Id 
		, rc.service_Name                                                          
		, fcc.DialedPhoneNumber 
		, rc.Call_Date 
into #sms_raw 
FROM DW_MSTR_DM.dbo.RadiusCall rc WITH (NOLOCK) 
	JOIN DW_MSTR_DM.dbo.FactCustomerCall fcc WITH (NOLOCK) ON rc.Session_Id = fcc.SessionId
WHERE rc.livevox_result IN ('SMS MT Delivered','SMS MT Failed') 
		AND rc.call_date >= '2023-06-01' 
		AND fcc.KeyDate_CallDate >= 20230601 

drop table if exists #sms 

SELECT s.keycustomer
		, s.keysourcesystem
		, dcl.ClientParent
		, dcl.ClientStreamID
		, dcl.ClientStream
		, dcl.ClientSegmentationGroup
		, dcl.KeyClient
		, s.Call_Date as sms_send_date
		, dcu.CustomerId
		, dcu.ClientId  
		, s.Service_Id 
		, s.service_Name                                                          
		, s.DialedPhoneNumber  
INTO #sms 
FROM #sms_raw s 
	JOIN DW_MSTR_DM.dbo.DimCustomer dcu WITH (NOLOCK) ON s.keycustomer = dcu.KeyCustomer
	JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) ON dcu.keyclient = dcl.keyclient 
WHERE dcl.ClientStreamId not in ( 'IMCOX' ) 
ORDER BY s.keycustomeR, s.DialedPhoneNumber, s.Call_Date  

--drop index #t_#sms on #sms
create index #t_#sms on #sms (keycustomer,DialedPhoneNumber,sms_send_date) --include (keycustomer)
 
DROP TABLE IF EXISTS #sms_partitioned 

SELECT s.* 
	, RANK () OVER (PARTITION BY s.keycustomer,s.DialedPhoneNumber ORDER BY s.sms_send_date) as rank_dt 
	, ROW_NUMBER () OVER (PARTITION BY s.keycustomer,s.DialedPhoneNumber ORDER BY s.sms_send_date) as r_dt  
	, LAG(s.KeyCustomer, 1) over (order by s.Keycustomer,s.DialedPhoneNumber, s.sms_send_date) prev_customer 
	, LAG(s.sms_send_date,1) OVER (ORDER BY s.keycustomer, s.DialedPhoneNumber, s.sms_send_date) prev_sms_date 
	, CASE WHEN LAG(s.KeyCustomer, 1) over (order by s.Keycustomer,s.DialedPhoneNumber, s.sms_send_date) = s.KeyCustomer 
		THEN DATEDIFF(dd,LAG(s.sms_send_date,1) OVER (ORDER BY s.keycustomer,s.DialedPhoneNumber, s.sms_send_date), s.sms_send_date)  
		ELSE NULL  
	END exec_interval   
INTO #sms_partitioned  
FROM #sms s  

CREATE NONCLUSTERED INDEX #t#sms_partitioned
ON #sms_partitioned ([service_Name],[exec_interval],[sms_send_date])
INCLUDE ([keycustomer],[keysourcesystem],[ClientParent],[ClientStreamID],[ClientStream],[CustomerId],[ClientId],[Service_Id],[DialedPhoneNumber],[r_dt],[prev_sms_date])

/*   
	interval exceptions  
*/ 
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_SMS_Threshold_Excpt 
(
	SourceSystem,KeyCustomer,CustomerID,ClientID,DialedPhoneNumber,ClientParent,ClientStreamId,ClientStream,Service_ID,Service_Name,SMS_Send_Date,SMS_Prev_Date,Rank_DT,
	Exec_Interval,Interval_Threshold,Attempt_Threshold,Interval_Exception,Attempt_Exception,Attempt_Limit_Reached 
)
SELECT ss.SourceSystem, s.KeyCustomer, s.CustomerId, s.ClientId, s.DialedPhoneNumber, s.ClientParent, s.ClientStreamId, s.ClientStream, s.Service_Id, s.Service_Name, s.SMS_Send_Date, 
	prev_sms_date, r_dt rank_dt, Exec_Interval, a.Interval, a.AttemptLimit, 
	CASE WHEN exec_interval > a.Interval THEN 1 ELSE 0 END Interval_Exception, 
	CASE WHEN r_dt > a.AttemptLimit THEN 1 ELSE 0 END Limit_Exception, 
	CASE WHEN r_dt = a.AttemptLimit THEN 1 ELSE 0 END Attempt_Limit_Reached   
FROM #sms_partitioned s    
	JOIN DW_MSTR_DM.dbo.DimSourceSystem ss on ss.KeySourceSystem = s.KeySourceSystem 
	JOIN DW_MSTR_DM.dbo.DimAttemptThresholds a WITH (NOLOCK) ON s.Service_Name LIKE a.expr  
WHERE a.IsActive = 1 
	AND sms_send_date >= ISNULL(@max_date, '2023-06-01') 
	AND exec_interval IS NOT NULL 
	AND (s.exec_interval > a.Interval 
			OR s.r_dt >= a.AttemptLimit)    
  
END 










