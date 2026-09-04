





--SET QUOTED_IDENTIFIER ON
--GO



CREATE PROCEDURE  [dbo].[sp_insert_fact_dial_excpt_1P_client]
--ALTER PROCEDURE  [dbo].[sp_insert_fact_dial_excpt_1P_client]
    
	 @StartDateTime DATETIME = NULL,
	 @end datetime = NULL
	
AS
/*
Object: sp_insert_fact_dial_excpt_1P_client

Description: Identify and insert dialer exceptions for Amex 1P new rules into fact_dial_excpt

Author			Date		Description
Amod Ramugade	12/24/2025	Created

*/

BEGIN
	SET NOCOUNT ON;

	---DECLARE  @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id  IN (35,36,37) 
AND KeySourceSystem = 3
---AND all_client_flag = 0

-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day, dss.keysourcesystem
, dde.dlr_expt_id 
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
WHERE 
dde.dlr_expt_id  IN (35,36,37) 
AND 
dss.KeySourceSystem = 3
AND dde.all_client_flag = 0

    IF OBJECT_ID('tempdb..#temp_fcc') IS NOT NULL DROP TABLE #temp_fcc;

	SELECT 
			fct.KeyCustomerCall
			, fct.KeyCustomer
		    , fct.KeySourceSystem
		    , fct.KeyDate_CallDate
			, fct.KeyEmployee
			, fct.CallStartTime
			, fct.DialedPhoneNumber
			, fct.DialedAreaCode
			, fct.SessionId
			, case when fct.IsRPC=1 then 1 else 0 end as contact_flag
			, fct.CallSeconds
			, fct.IsRPC
			, fct.zip
INTO #temp_fcc
    FROM DW_MSTR_DM.dbo.FactCustomerCall fct WITH (NOLOCK)
    WHERE
   fct.IsOutbound = 1
        AND fct.KeyCustomer > 0
		AND fct.KeySourceSystem = 3
		AND fct.KeyDate_CallDate BETWEEN CONVERT(VARCHAR,CAST(ISNULL(@StartDateTime, GETDATE()) - 31 AS DATE),112) AND CONVERT(VARCHAR,CAST(ISNULL(@StartDateTime, GETDATE()) AS DATE),112) 
	

	drop table if exists #temp_rc ;
  
	select
		  rc.service_name
        , rc.service_id
        , rc.Call_Center_Name
        , rc.lv_client_name
		, rc.Session_Id
        , rc.Livevox_Result
		, rc.Call_Date
			into #temp_rc  
	from DW_MSTR_DM.dbo.RadiusCall rc with (nolock) 
	where rc.Call_Date between isnull(@end, GETDATE()) - 31 and isnull(@end, GETDATE()) 
        -- SMS suppression
        AND rc.service_name NOT LIKE '%HTI%'
		AND rc.LV_Client_Name = 'Veldos'
        AND rc.livevox_result NOT LIKE 'SMS%'
        AND rc.livevox_result NOT LIKE '%Text%'

----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;
	SELECT 
			fct.KeyCustomerCall
			, cust.ClientId
			, fct.KeyCustomer
			, cust.CustomerId
			, fct.KeyEmployee
			, dss.KeySourceSystem
			, dss.SourceSystem
			, fct.CallStartTime
			, dt.WeekId
			, fct.DialedPhoneNumber
			, fct.DialedAreaCode
			, fct.SessionId
			, case when fct.IsRPC=1 then 1 else 0 end as contact_flag
			, de.EmployeeId
			, fct.CallSeconds
			, dcl.ClientParent
			, fct.IsRPC
			, rc.Call_Center_Name
	--isrpc_lag1 indicates previous call was an RPC without a requested callback
			, LAG(CASE WHEN rc.Livevox_Result <> 'AGENT - CUST RPC 12' THEN fct.IsRPC ELSE 0 END ,1) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime) AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG(fct.CallStartTime,1) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime, fct.SessionId) AS callstarttime_lag1
    --left_msg_lag1 for indicates previous call was Left Message
			,LAG(CASE WHEN rc.Livevox_Result IN ('AGENT - CUST 11','AGENT - CUST 2', 'AGENT - Left Message Machine', 'AGENT - Left Message Machine', 'Machine') THEN 1 ELSE 0 END, 1) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime ) AS left_msg_lag1
	--callstarttime_lag4 for calculating whether called  more than 4 times per day on any given phone number
			, LAG(fct.CallStartTime,4) OVER(PARTITION BY fct.DialedPhoneNumber ORDER BY fct.CallStartTime, fct.SessionId) AS callstarttime_lag4
	--callstarttime_lag9 for calculating whether called  more than 9 times per day at account level
			, LAG(fct.CallStartTime,9) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime, fct.SessionId) AS callstarttime_lag9
    --calls with Dialed NV area codes
			, case when ac.StateCode='NV' then 1 else 0 end as statecode_nv
	--calls with Dialed CT area codes
			, case when ac.StateCode='CT' then 1 else 0 end as statecode_ct
	--calls with Dialed DC area codes
			, case when ac.StateCode='DC' then 1 else 0 end as statecode_dc
	--calls with Dialed NV zip codes
			, case when zs.CustomerState='NV' then 1 else 0 end as custstate_nv
	--calls with Dialed CT zip codes
			, case when zs.CustomerState='CT' then 1 else 0 end as custstate_ct
	--calls with Dialed DC zip codes
			, case when zs.CustomerState='DC' then 1 else 0 end as custstate_dc
			
	INTO #calls 
	
	FROM #temp_fcc fct
			  inner join
		 #temp_rc rc  on fct.SessionId = rc.Session_Id
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
/*
			  LEFT  JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and fct.DialedPhoneNumber=RP.PhoneNumber
*/
		      left  join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left  join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		  left join 
		 [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup on lup.ClientCode= cust.ClientId
                                                                                    and cust.SourceSystem= 'AMEX Latitude'
																					and lup.FirstPartyFlag = 1
		 left  join 
		 CLIENT_ANALYTICS.[dbo].[AreaCodes]  ac (nolock) on fct.DialedAreaCode = ac.areacode AND ac.Is_Amex = 1


		     LEFT JOIN CLIENT_ANALYTICS.dbo.zipcodes zs (nolock)
        ON fct.zip = zs.zip AND zs.customerstate <> 'NY'
	WHERE dt.CalendarDate >= isnull(@StartDateTime,GETDATE())-31                
	   -- AND fct.IsOutbound=1
	   -- AND rc.LV_Client_Name = 'Veldos'
		--and rc.service_name not like '%HTI%'
		and lup.FirstPartyFlag = 1
			
		;

		CREATE CLUSTERED INDEX IX_calls_optimized
ON #calls (CallStartTime, KeyCustomer);

CREATE NONCLUSTERED INDEX IX_calls_phone_opt
ON #calls (DialedPhoneNumber, CallStartTime)
INCLUDE (KeyCustomer);

-------Insert new recs for First ever WPCs from past 7 days into Build_Amex_1P_First_Ever_WPC_Calls table-----------------------------------------------------------------------------------
INSERT INTO CLIENT_ANALYTICS.dbo.Build_Amex_1P_First_Ever_WPC_Calls (KeyCustomer, Callstarttime, DialedPhoneNumber, insert_date)
	 	
		SELECT fct.KeyCustomer, MIN(fct.CallStartTime) AS First_Ever_WPC, fct.DialedPhoneNumber , GETDATE() AS insert_date
		FROM #temp_fcc fct
			  inner join
		 #temp_rc rc (nolock) on fct.SessionId=rc.Session_Id AND rc.Call_Date >= ISNULL(@StartDateTime,GETDATE())-7
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
/*
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and 	fct.DialedPhoneNumber=RP.PhoneNumber
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem 
*/
		 	  inner join 
		 [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup on lup.ClientCode= cust.ClientId
                                                                                    and cust.SourceSystem= 'AMEX Latitude'
																					and lup.FirstPartyFlag = 1
                                                                                    --and (lup.FirstPartyFlag <> 1 
																					--     or lup.FirstPartyFlag is NULL)
		 LEFT OUTER JOIN 
		 CLIENT_ANALYTICS.dbo.Build_Amex_1P_First_Ever_WPC_Calls (NOLOCK) w 
		 ON fct.KeyCustomer = w.KeyCustomer 
		 AND  fct.CallStartTime >= w.callstarttime 
		 AND fct.DialedPhoneNumber = w.dialedphonenumber
WHERE dt.CalendarDate >= isnull(@StartDateTime,GETDATE())-7
	    --AND  fct.IsOutbound=1
	    --AND rc.LV_Client_Name = 'Veldos'
		AND rc.Livevox_Result  = 'AGENT - WRONG NUMBER'
		--and rc.service_name not like '%HTI%'
		--and lup.FirstPartyFlag = 1	
		---and (lup.FirstPartyFlag <> 1 or lup.FirstPartyFlag is NULL)
		AND w.KeyCustomer IS NULL
GROUP BY 	  fct.KeyCustomer		
			, fct.DialedPhoneNumber
          ;

---------------------------Wrong Numbers on which Customer provided authorization to call again-------------------------------------------------------------------------------------
DELETE w
FROM CLIENT_ANALYTICS.dbo.Build_Amex_1P_First_Ever_WPC_Calls w 
inner join dw_MSTR_DM.dbo.RadiusPhone rp (NOLOCK) 
on w.KeyCustomer = RP.KeyCustomer
 and w.DialedPhoneNumber = RP.PhoneNumber
 and rp.KeySourceSystem = 3
 and rp.PhoneType IN ('Good', 'Verified', 'Consent', 'Call Only')


	--EXCEPTIONS
	--identify exceptions for each rule
	---WITH exceptions AS
	---(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
	------dlr_expt_id=35:  Client – Amex 1P – Post-RPC 0 attempts per day
				SELECT 35 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE isrpc_lag1=1
			  AND CAST(c.callstarttime_lag1 AS DATE) = CAST(c.CallStartTime AS DATE)
			  
/*
		UNION
    ----dlr_expt_id=12:  Client – Amex – Post-Left Msg 0 attempts per day	
		SELECT 12 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
			WHERE 
			c.left_msg_lag1 = 1
	          AND CAST(c.callstarttime_lag1 AS DATE) = CAST(c.CallStartTime AS DATE)
*/			  

			  UNION
    ---dlr_expt_id=36:  Client – Amex 1P – Post-WPC 0 attempts for lifetime		
		SELECT 36 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			  
		FROM #calls c INNER JOIN CLIENT_ANALYTICS.dbo.Build_Amex_1P_First_Ever_WPC_Calls (NOLOCK) w
		ON c.KeyCustomer = w.keycustomer
		AND c.DialedPhoneNumber = w.DialedPhoneNumber
		AND c.CallStartTime > w.CallStartTime
		AND c.DialedPhoneNumber <> 0
		
	---)

/*
	UNION
	------dlr_expt_id=22:  Client – Amex – NV Area Code
				SELECT 22 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE c.statecode_nv = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')
		
		UNION
	------dlr_expt_id=23:  Client – Amex – NV Zip Code
				SELECT 23 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE c.custstate_nv = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')

		UNION
	------dlr_expt_id=24:  Client – Amex – CT Area Code
				SELECT 24 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE c.statecode_ct = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')

		UNION
	------dlr_expt_id=25:  Client – Amex – CT Zip Code
				SELECT 25 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE c.custstate_ct = 1
		AND c.Call_Center_Name IN ('India', 'Non Reg F')

	UNION
	------dlr_expt_id=26:  Client – Amex – 4 attempts per day by Phone Number
				SELECT 26 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE CAST(c.callstarttime_lag4 AS DATE) = CAST(c.CallStartTime AS DATE)

		UNION
		------dlr_expt_id=27:  Client – Amex – 9 attempts per day by Account
				SELECT 27 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE CAST(c.callstarttime_lag9 AS DATE) = CAST(c.CallStartTime AS DATE)
*/

		UNION
		------dlr_expt_id=37:  Client – Amex 1P – DC 4 Attempts and 1 RPC per Week
				SELECT 37 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , C.keysourcesystem
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
		WHERE c.statecode_dc+c.custstate_dc>=1     --either area code or custstate=DC
		      and
			  (
  			     --previous RPC is within 7 days
				 (
		          isrpc_lag1=1 
			      and datediff(day,cast(c.callstarttime_lag1 as date),CAST(c.CallStartTime AS DATE))<=7
			     )
			     or
				 --5th previous attempt is within 7 days
				 (
				  datediff(day,cast(c.callstarttime_lag4 as date),CAST(c.CallStartTime AS DATE))<=7
				 )
			  )


---SELECT dlr_excpt_id, count(*)  FROM #exceptions group by dlr_excpt_id order by dlr_excpt_id

		
--------------------------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count] for Yesterday----------------------------

INSERT INTO  CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count]
SELECT calldate
,keysourcesystem
,dlr_expt_id
,Count_of_Exceptions
,Insert_Date
FROM
(
SELECT #t.day AS calldate
,#t.keysourcesystem AS keysourcesystem
,#t.dlr_expt_id AS dlr_expt_id
,ISNULL(SUM(E.Count_of_Exceptions),0) AS Count_of_Exceptions
,GETDATE() AS Insert_Date
,c.No_of_Calls
FROM #t
LEFT JOIN 
(SELECT CAST(exc.CallStartTime AS DATE) calldate
,exc.keysourcesystem
,exc.dlr_excpt_id
,COUNT(*) AS Count_of_Exceptions
FROM #exceptions exc 
group by CAST(exc.CallStartTime AS DATE)
,exc.keysourcesystem
,exc.dlr_excpt_id
) E
ON CAST(e.calldate AS DATE) = #t.day
	AND e.keysourcesystem = #t.keysourcesystem
	AND e.dlr_excpt_id = #t.dlr_expt_id
LEFT JOIN 
(
SELECT 
dt.CalendarDate AS calldate
, fct.KeySourceSystem
, COUNT(*) AS No_of_Calls 
FROM  DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
inner join
DW_MSTR_DM.dbo.DimDate dt (NOLOCK) 
ON fct.KeyDate_CallDate = dt.KeyDate
WHERE dt.CalendarDate  = CAST(ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS DATE)
GROUP BY dt.CalendarDate  , fct.KeySourceSystem 
)c
ON #t.day = CAST(c.calldate AS DATE) 
	AND #t.keysourcesystem = c.KeySourceSystem

GROUP BY 
#t.day 
,#t.keysourcesystem
,#t.dlr_expt_id   
,c.No_of_Calls
)f
WHERE ISNULL(f.No_of_Calls,0) > 0
AND calldate >= '2026-01-01'


	

------------------------------------Insert Calls Exceptions into CLIENT_ANALYTICS.dbo.fact_dial_excpt------------------------------------------------
INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,
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
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt fde (NOLOCK) ON exc.keycustomercall=fde.keycustomercall
	WHERE fde.keycustomercall IS NULL 
		  AND fde.call_history_fact_id IS NULL   
		  AND exc.calldate >= '2026-01-01'
                           

END;









