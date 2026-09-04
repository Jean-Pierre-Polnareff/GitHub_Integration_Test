



CREATE PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb]
	
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb

Description: Identify and insert dialer exceptions for CFPB new rules into fact_dial_excpt

Author			Date		Description
Mike Campbell	05/03/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	--	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

	-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
	DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
	WHERE calldate = isnull(@end
							,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
	AND dlr_excpt_id IN (9,10) 
	AND KeySourceSystem  NOT IN (0,4)                                 ---IN (1,2,3,7,10)

	-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
		IF OBJECT_ID('tempdb..#t') IS NOT NULL
			DROP TABLE #t;
	SELECT isnull(@end
				   ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day
		   , dss.keysourcesystem
		   , dde.dlr_expt_id 
	INTO #t 
	FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
	CROSS JOIN 
	CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
	WHERE dde.dlr_expt_id IN (9,10) 
	AND dss.KeySourceSystem NOT IN (0,4)                                 ---IN (1,2,3,7,10)
	AND dde.all_client_flag = 1
	----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = '
		Veldos calls found for clientids not documented by Bilal or Susheel.
		See attached.

		Please contact analytics@radiusgs.com with questions or issues.';
	
	drop table if exists #temp_fcc 

	select fcc.KeyCustomerCall, 
		fcc.KeyCustomer, 
		fcc.Consumer_ID, 
		fcc.KeySourceSystem, 
		fcc.KeyEmployee,  
		fcc.CallStartTime, 
		fcc.DialedPhoneNumber, 
		fcc.DialedAreaCode, 
		fcc.SessionId, 
		fcc.IsRPC, 
		fcc.CallSeconds, 
		fcc.KeyDate_CallDate,  
		fcc.IsOutbound
	into #temp_fcc 
	from DW_MSTR_DM.dbo.FactCustomerCall fcc with (nolock)  
	where fcc.KeyDate_CallDate between convert(varchar,cast(isnull(@end, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@end, GETDATE()) as date),112)
		and fcc.IsOutbound = 1
		and fcc.callcentername NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party 

	drop table if exists #temp_rc 
  
	select rc.Session_Id,   
		rc.LV_Client_Name, 
		rc.Call_Date, 
		rc.Livevox_Result,  
		rc.service_name, 
		rc.call_center_name   
	into #temp_rc  
	from DW_MSTR_DM.dbo.RadiusCall rc with (nolock)
    LEFT JOIN CLIENT_ANALYTICS.dbo.Dial_Excpt_LiveVox_Rules lvr with (nolock)
        ON rc.LV_Client_Name = lvr.lv_client_name
        AND rc.Livevox_Result = lvr.livevox_result
		AND lvr.IS_CFPB = 1	
    LEFT JOIN CLIENT_ANALYTICS.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs with (nolock)
	    ON rc.Service_Id = dvs.Service_Id 
		AND dvs.Is_Exclude = 1 
		AND dvs.IS_CFPB = 1
	where rc.Call_Date between isnull(@end, GETDATE()) - 31 and isnull(@end, GETDATE()) 
		AND (
             dvs.is_exclude IS NULL
            )
		AND rc.service_name not like '%HTI%'
		AND rc.livevox_result  NOT LIKE 'SMS%' AND rc.livevox_result NOT LIKE '%Text%' 
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.service_name ELSE '' end 
				NOT IN('Payment Verification Manual')
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.call_center_name ELSE '' end 
				NOT IN('Non Reg F')
		        -- LiveVox rules (config-driven)
		AND (
            -- If rule exists and is excluded → filter out
             lvr.is_excluded IS NULL
            )
		AND rc.call_center_name  NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party 
		AND CASE WHEN rc.LV_Client_Name = 'RGS-THI' THEN rc.Call_Center_Name ELSE '' END NOT IN ('ATT 1P') -- per Brett on 2nd Jul 2026 for Reg-F, excludes call center ATT 1P on RGS-THI Portal

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	SELECT 
			fct.KeyCustomerCall
			, cust.ClientId
			, case when dss.SourceSystem='Amex Latitude' then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			       else CAST(fct.KeyCustomer AS VARCHAR)
				   end as KeyCustomer
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
	--7th previous call start time since limit of 7 calls in 7 days.  If 7th call in less than 7 days, exception.
			, LAG(fct.CallStartTime,7) OVER(PARTITION BY case when dss.SourceSystem='Amex Latitude' 
			                                                  then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			                                                  else CAST(fct.KeyCustomer AS VARCHAR)
				                                              end 
			                                ORDER BY fct.CallStartTime, fct.sessionid) AS callstarttime_lag7
/* removing isrpc_lag1 and callstarttime_lag1 as it's not being used to identify the exceptions
	--isrpc_lag1 indicates previous call was an RPC
			, LAG(fct.IsRPC,1) OVER(PARTITION BY case when dss.SourceSystem='Amex Latitude' 
			                                          then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			                                          else CAST(fct.KeyCustomer AS VARCHAR)
				                                      end 
			                        ORDER BY fct.CallStartTime, fct.sessionid) AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG(fct.CallStartTime,1) OVER(PARTITION BY case when dss.SourceSystem='Amex Latitude' 
			                                                  then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			                                                  else CAST(fct.KeyCustomer AS VARCHAR)
				                                              end 
			                                ORDER BY fct.CallStartTime, fct.sessionid) AS callstarttime_lag1
*/											 
			, CASE WHEN rc.LV_Client_Name ='veldos' AND ccsv.client_id IS NULL THEN 0 ELSE 1 END AS valid_clientid
		    , rc.LV_Client_Name
		    , rc.Livevox_Result
	INTO #calls  
	FROM #temp_fcc fct (NOLOCK)   
			  inner join
		 #temp_rc rc (nolock) on fct.SessionId = rc.Session_Id 
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
/*
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and 	fct.DialedPhoneNumber=RP.PhoneNumber
*/
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		      LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.DimCustomerProduct dcp (NOLOCK) on cust.KeyCustomer=dcp.KeyCustomer 

    LEFT JOIN CLIENT_ANALYTICS.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress ccsv with (nolock)
        ON dcl.ClientId = ccsv.client_id AND ccsv.keysourcesystem = 3 
           AND ccsv.Is_Valid = 1 AND ccsv.IS_CFPB = 1

	LEFT JOIN CLIENT_ANALYTICS.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce with (nolock) 
	    ON dcl.ClientId = cvce.client_id AND cvce.Is_Exclude = 1 AND cvce.IS_CFPB = 1
         
	WHERE ISNULL(dcp.ProductType,'') NOT IN ('EX','SB','SM','SR','AB','HC','HP','BT','CB','CC','CP','CR','DV')
          
		AND (CASE WHEN LEFT(fct.DialedPhoneNumber,3)='800' AND cust.CustomerId=0 THEN 1 ELSE 0 END) = 0
		AND cvce.client_id IS NULL
/*		  
		AND dcl.clientid NOT IN  ('SNBCEP','DCMYSP','DCABBP','DCABYP','DCAD2P','DCADSP','DCAFDP','DCAFLP','DCAFSP','DCAKDP',
								'DCAOMP','DCAPDP','DCAVAP','DCCSSP','DCMYSP','SNBCE1','ATTMOB','ATRAB1')
*/

		--AND (dt.CalendarDate >= '11/30/21');


	--Send email on invalid Amex clientids
    --populate tmp tbl
	drop table if exists dw_staging.dbo.TMP_axp_unlist_clientid;

    select SourceSystem
           , ClientId
     	   , count(*) as calls
    into dw_staging.dbo.TMP_axp_unlist_clientid
    from #calls
    where valid_clientid=0
          and len(clientid)>0
    group by SourceSystem
           , ClientId;
		    
	--send email
	if (select count(*) from dw_staging.dbo.TMP_axp_unlist_clientid)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = @@SERVERNAME,--'DFW2-BISQL-001',
		@from_address ='dw@radiusgs.com',
		@recipients = 
		'dw@radiusgs.com',

		@subject = 'Veldos Non-Listed ClientIDs for Call Exception Reporting',

		@body = @body1,

		@query = 'select *
		from dw_staging.dbo.TMP_axp_unlist_clientid',

		@query_result_header=1, @attach_query_result_as_file=1;

	--CLEAN OUT INVALID AXP CLIENTID CALLS
	delete from #calls where valid_clientid=0;


	--EXCEPTIONS
	--identify exceptions for each rule

	---WITH exceptions AS
	---(
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
		       join
			 #calls c1 on c.KeyCustomer=c1.KeyCustomer 
			              and c1.CallStartTime<c.CallStartTime
			              and datediff(day,c1.callstarttime,c.callstarttime)<7
						  --and c1.IsRPC=1
						  and 
						  (
						  c1.IsRPC=1
						  OR (
						  c1.Livevox_Result = 'AGENT - CUST 3'
						  and c1.LV_Client_Name IN ('RGS-CCS', 'RGS-THI','RGS_Frontline','ISSManualDial')
						  )
						)
	---)

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
WHERE dt.CalendarDate  = CAST(isnull(@end
                                     ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) 
							   AS DATE)
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



--------------------------------------------------Insert to fact_dial_excpt--------------------------------------------------
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

END;

