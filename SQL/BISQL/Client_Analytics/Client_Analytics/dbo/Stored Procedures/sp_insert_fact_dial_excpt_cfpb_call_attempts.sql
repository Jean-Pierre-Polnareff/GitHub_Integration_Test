USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_cfpb_call_attempts]    Script Date: 12/13/2024 9:09:34 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







 
CREATE PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb_call_attempts]
	
	@StartDateTime DATETIME = NULL

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb_call_attempts

Description: Identify and insert call attempts for dialer exceptions for CFPB new rules into fact_dial_expt_call_attempts

Author			Date		Description
Amod Ramugade	03/14/2023	Created
*/

BEGIN
	SET NOCOUNT ON;

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts] for last 31 days------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts]
WHERE calldate  > isnull(@StartDateTime,GETDATE())-31
AND dlr_excpt_id  IN (9,10) 
AND KeySourceSystem NOT IN (0,4)                                 ---IN (1,2,3,7,10)


----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

		
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
	where fcc.KeyDate_CallDate between convert(varchar,cast(isnull(@StartDateTime, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@StartDateTime, GETDATE()) as date),112)
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
	where rc.Call_Date between isnull(@StartDateTime, GETDATE()) - 31 and isnull(@StartDateTime, GETDATE())
		AND (
             dvs.is_exclude IS NULL
            )	 
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.service_name ELSE '' end 
		         NOT IN ('Payment Verification Manual')
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.call_center_name ELSE '' end 
		        NOT IN ('Non Reg F')
		        -- LiveVox rules (config-driven)
		AND (
            -- If rule exists and is excluded → filter out
             lvr.is_excluded IS NULL
            )
        AND rc.service_name not like '%HTI%'
        AND rc.livevox_result  NOT LIKE 'SMS%' AND rc.livevox_result NOT LIKE '%Text%' 
		AND rc.call_center_name NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party
		AND CASE WHEN rc.LV_Client_Name = 'RGS-THI' THEN rc.Call_Center_Name ELSE '' END NOT IN ('ATT 1P') -- per Brett on 2nd Jul 2026 for Reg-F, excludes call center ATT 1P on RGS-THI Portal		 

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;
 
	SELECT cust.ClientId
			, dss.KeySourceSystem
			, dss.SourceSystem
			, CAST(fct.CallStartTime AS DATE) as calldate
			, dcl.ClientParent
			, fct.IsRPC
			, CASE WHEN rc.LV_Client_Name ='veldos' AND ccsv.client_id IS NULL THEN 0 ELSE 1 END AS valid_clientid
			, count(*) count_of_recs
			, rc.LV_Client_Name
		    , rc.Livevox_Result
	INTO #calls  
	FROM #temp_fcc fct (NOLOCK)
			  inner join
		 #temp_rc rc (nolock) on fct.SessionId=rc.Session_Id 
		                                          AND rc.Call_Date between isnull(null, GETDATE()) - 31 and isnull(null, GETDATE()) 
												  AND fct.KeyDate_CallDate between convert(varchar,cast(getdate() - 31 as date),112) and convert(varchar,cast(getdate() as date),112) 
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
         
	WHERE ISNULL(dcp.ProductType,'') NOT IN('EX','SB','SM','SR','AB','HC','HP','BT','CB','CC','CP','CR','DV')
          AND (CASE WHEN LEFT(fct.DialedPhoneNumber,3)='800' AND cust.CustomerId=0 THEN 1 ELSE 0 END) = 0
		  AND cvce.client_id IS NULL
/*
		  AND dcl.clientid NOT IN('SNBCEP','DCMYSP','DCABBP','DCABYP','DCAD2P','DCADSP','DCAFDP','DCAFLP','DCAFSP','DCAKDP',
                                  'DCAOMP','DCAPDP','DCAVAP','DCCSSP','DCMYSP','SNBCE1','ATTMOB','ATRAB1')
*/
		  --AND (dt.CalendarDate>='11/30/21')
		 GROUP BY
			 cust.ClientId
			, dss.KeySourceSystem
			, dss.SourceSystem
			, CAST(fct.CallStartTime AS DATE) 
			, dcl.ClientParent
			, fct.IsRPC
			, CASE WHEN rc.LV_Client_Name ='veldos' AND ccsv.client_id IS NULL THEN 0 ELSE 1 END
			, rc.LV_Client_Name
		    , rc.Livevox_Result;

	
	
	
	--CLEAN OUT INVALID AXP CLIENTID CALLS
	delete from #calls where valid_clientid=0;


	--EXCEPTIONS CALL ATTEMPTS
	--identify call attempts for each exception rule


	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlr_expt_id=9:  CFPB - 7 attempts per 7 sliding
		SELECT 9 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		INTO #exceptions
		FROM #calls 
		

		UNION
		--dlr_expt_id=10:  CFPB - Post-RPC 0 attempts 7 days
		SELECT 10 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		FROM #calls 
		    where IsRPC=1
					OR (
						  Livevox_Result = 'AGENT - CUST 3'
						  and LV_Client_Name IN ('RGS-CCS', 'RGS-THI','RGS_Frontline','ISSManualDial')
					    )
						



--------------------------------------------------Insert to fact_dial_expt_call_attempts--------------------------------------------------
	  INSERT INTO  client_analytics.dbo.fact_dial_expt_call_attempts (dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		  )
		  SELECT 
		  dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
			   FROM #exceptions

END;


