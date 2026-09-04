





CREATE PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb_client]
	
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb_client

Description: Identify and insert dialer exceptions for CFPB new rules at Consumer ID level into fact_dial_excpt for specific clients 
i.e. Sallie Mae (first placement 2/15/25) (on new Portal, SDProd), Navient (RGS CCS Portal, thirdprod) and Southwood (RGS CCS Portal, thirdprod) 

Author			Date		Description
Amod Ramugade	05/27/2025	Created
*/

BEGIN
	SET NOCOUNT ON;

--	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = isnull(@end
                        ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id IN (33,34) 
AND KeySourceSystem  IN (2,10)

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
WHERE dde.dlr_expt_id IN (33,34) 
AND dss.KeySourceSystem IN (2,10)
AND dde.all_client_flag = 0
----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	
	drop table if exists #temp_fcc 

	select fcc.KeyCustomerCall 
		, fcc.KeyCustomer 
		, fcc.KeySourceSystem
		, fcc.KeyEmployee
		, fcc.CallStartTime
		, fcc.DialedPhoneNumber
		, fcc.DialedAreaCode
		, fcc.SessionId
		, fcc.IsRPC
		, fcc.CallSeconds
		, fcc.KeyDate_CallDate
		, fcc.IsOutbound
	into #temp_fcc 
	from DW_MSTR_DM.dbo.FactCustomerCall fcc with (nolock)  
	where fcc.KeyDate_CallDate between convert(varchar,cast(isnull(@end, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@end, GETDATE()) as date),112)
		and fcc.IsOutbound = 1
		and fcc.callcentername NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party 
		AND fcc.KeySourceSystem IN (2,10)

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
	/*
		AND rc.service_name NOT IN ('DEC_1P-DCA CTD','DEC_1P-DCA HCI','DEC_1P-DCA Inbound','DEC_1P-DCA Manual','DEC_1P-DCA QC',
										 'DEC_1P-DJO HCI','DEC_1P-DJO Inbound','DEC_1P-DJO Manual','DEC_1P-DJO QC','DEC_1P-DJO UAM',
										 'LXA_3P-ACB_DSA_CTD','LXA_3P-ACB_DSA_Inbound','LXA_3P-TD_DSA_CTD','LXA_3P-TD_DSA_Inbound','IDL Artiva SMS' )
    */
		AND (
             dvs.is_exclude IS NULL
            )
		AND rc.service_name not like '%HTI%'
		AND rc.livevox_result  NOT LIKE 'SMS%' AND rc.livevox_result NOT LIKE '%Text%' 
/*
		AND rc.LV_Client_Name IN ('RGS-CCS', 'RGS-THI','ISSManualDial') 
		AND rc.Livevox_Result 	NOT IN ('AGENT - CUST 10','AGENT - Attorney Handling','AGENT - CUST 11','AGENT - CUST 8','AGENT - CUST RPC 18',
					'Busy','Fax','Invalid Phone Number','AGENT - CUST RPC PTP 6','Busy','Invalid Phone Number') 
*/
		AND rc.call_center_name  NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party
		AND CASE WHEN rc.LV_Client_Name = 'RGS-THI' THEN rc.Call_Center_Name ELSE '' END NOT IN ('ATT 1P') -- per Brett on 2nd Jul 2026 for Reg-F, excludes call center ATT 1P on RGS-THI Portal 
		        -- LiveVox rules (config-driven)
		AND (
            -- If rule exists and is excluded → filter out
             lvr.is_excluded IS NULL
            )

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	SELECT 
			fct.KeyCustomerCall
			, cust.ClientId
			, case when dss.SourceSystem='Amex Latitude' then isnull(cust.ConsumerID,CAST(fct.KeyCustomer AS VARCHAR))
			       else CAST(fct.KeyCustomer AS VARCHAR)
				   end as KeyCustomer
			, cust.CustomerId 
			, cust.ConsumerID  
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
			--, dr.Dialer_Result
	--7th previous call start time since limit of 7 calls in 7 days.  If 7th call in less than 7 days, exception.
			, LAG(fct.CallStartTime,7) OVER(PARTITION BY ISNULL(cust.ConsumerID,CAST(fct.KeyCustomer AS VARCHAR))			                                              
			                                ORDER BY fct.CallStartTime, fct.SessionId) AS callstarttime_lag7
	/*
	--isrpc_lag1 indicates previous call was an RPC
			, LAG(fct.IsRPC,1) OVER(PARTITION BY isnull(cust.ConsumerID,CAST(fct.KeyCustomer AS VARCHAR)) 
			                        ORDER BY fct.CallStartTime) AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG(fct.CallStartTime,1) OVER(PARTITION BY isnull(cust.ConsumerID,CAST(fct.KeyCustomer AS VARCHAR))
			                                ORDER BY fct.CallStartTime) AS callstarttime_lag1 
    */
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
         	  LEFT JOIN 
		 CLIENT_ANALYTICS.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce with (nolock) ON dcl.ClientId = cvce.client_id AND cvce.Is_Exclude = 1 AND cvce.IS_CFPB = 1

	WHERE ISNULL(dcp.ProductType,'') NOT IN ('EX','SB','SM','SR','AB','HC','HP','BT','CB','CC','CP','CR','DV')
          
		AND (CASE WHEN LEFT(fct.DialedPhoneNumber,3)='800' AND cust.CustomerId=0 THEN 1 ELSE 0 END) = 0
		AND cvce.client_id IS NULL
		/*  
		AND dcl.clientid NOT IN  ('SNBCEP','DCMYSP','DCABBP','DCABYP','DCAD2P','DCADSP','DCAFDP','DCAFLP','DCAFSP','DCAKDP',
								'DCAOMP','DCAPDP','DCAVAP','DCCSSP','DCMYSP','SNBCE1','ATTMOB','ATRAB1')
		*/
		AND (
		(
		dss.Keysourcesystem = 2
		AND  dcl.ClientStreamId IN ('IMNAVIENT', 'IMSOUTHWD')
		) OR dss.Keysourcesystem = 10
		);

	--EXCEPTIONS
	--identify exceptions for each rule

	---WITH exceptions AS
	---(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlr_expt_id=33:  CFPB - 7 attempts per 7 sliding (Consumer ID)
		SELECT 33 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.ConsumerID
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
			   , c.Livevox_Result
		INTO #exceptions
		FROM #calls c
		WHERE DATEDIFF(DAY,c.callstarttime_lag7,c.CallStartTime)<7

		UNION
		--dlr_expt_id=34:  CFPB - Post-RPC 0 attempts 7 days (Consumer ID)
		SELECT 34 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
			   , c.ConsumerID
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
			   , c.Livevox_Result
		FROM #calls c
		       join
			 #calls c1 on c.ConsumerID = c1.ConsumerID
			             -- and c.KeyCustomer=c1.KeyCustomer
			              and c1.CallStartTime<c.CallStartTime
			              and datediff(day,c1.callstarttime,c.callstarttime)<7
						  --and c1.IsRPC=1
						  and 
						  (
						  c1.IsRPC=1
						  OR (
						  c1.Livevox_Result = 'AGENT - CUST 3'
						  and c1.LV_Client_Name IN ('RGS-CCS', 'RGS-THI','ISSManualDial')
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
							   AND fct.KeySourceSystem IN (2,10)
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