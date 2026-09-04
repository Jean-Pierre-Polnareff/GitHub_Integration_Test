USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_call_attempts]    Script Date: 5/11/2026 10:56:30 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



ALTER PROCEDURE [dbo].[sp_insert_fact_dial_excpt_call_attempts]
	
	@StartDateTime DATETIME = NULL
		
AS

/* 
Object: sp_insert_fact_dial_excpt_call_attempts

Description: Identify and insert call attempts for dialer exceptions into fact_dial_expt_call_attempts

Author			Date		Description
Amod Ramugade	03/14/2023	Created
*/

BEGIN
	SET NOCOUNT ON;

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts] for last 31 days------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_expt_call_attempts]
WHERE calldate  > isnull(@StartDateTime,GETDATE()) - 31
AND dlr_excpt_id  IN (1,2,3,4,5,6,7,8,29) 
AND KeySourceSystem not in (0,4) 

----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------


    IF OBJECT_ID('tempdb..#temp_fcc') IS NOT NULL DROP TABLE #temp_fcc;

    SELECT 
          fct.KeyCustomerCall
		, fct.Consumer_ID
		, fct.KeyCustomer
		, fct.KeySourceSystem
		, fct.KeyDate_CallDate
        , fct.KeyEmployee
        , fct.CallStartTime
        , fct.DialedPhoneNumber
        , fct.DialedAreaCode
        , fct.SessionId		
		, fct.PhonePosition
		, fct.zip
		, fct.zip_scf
        , CASE WHEN fct.IsRPC = 1 THEN 1 ELSE 0 END AS contact_flag
        , fct.CallSeconds
        
    INTO #temp_fcc
    FROM DW_MSTR_DM.dbo.FactCustomerCall fct WITH (NOLOCK)
    WHERE 
        fct.IsOutbound = 1
        AND fct.KeyCustomer > 0
		AND fct.KeyDate_CallDate BETWEEN CONVERT(VARCHAR,CAST(ISNULL(@StartDateTime, GETDATE()) - 31 AS DATE),112) AND CONVERT(VARCHAR,CAST(ISNULL(@StartDateTime, GETDATE()) AS DATE),112) 
 
drop table if exists #temp_rc ;
   
	select
		  rc.service_name
        , rc.service_id
        , rc.call_center_id
        , rc.lv_client_name
		, rc.Session_Id
        , rc.Livevox_Result
			into #temp_rc  
	from DW_MSTR_DM.dbo.RadiusCall rc with (nolock) 
    LEFT JOIN DW_RETENTION.dbo.Dial_Excpt_LiveVox_Rules lvr
        ON rc.LV_Client_Name = lvr.lv_client_name
        AND rc.Livevox_Result = lvr.livevox_result
	where rc.Call_Date between isnull(@StartDateTime, GETDATE()) - 31 and isnull(@StartDateTime, GETDATE()) 
        -- SMS suppression
        AND rc.service_name NOT LIKE '%HTI%'
        AND rc.service_name <> 'IDL Artiva SMS'
        AND rc.livevox_result NOT LIKE 'SMS%'
        AND rc.livevox_result NOT LIKE '%Text%'
		        -- LiveVox rules (config-driven)
		AND (
            -- If rule exists and is excluded → filter out
             lvr.is_excluded IS NULL
            )
------------------------------------------------------------
    -- CALLS
    ------------------------------------------------------------
    IF OBJECT_ID('tempdb..#calls') IS NOT NULL DROP TABLE #calls;

SELECT --fct.KeyCustomerCall
		--, fct.Consumer_ID  
		--,
		 fct.KeySourceSystem
		, fct.KeyDate_CallDate
        --, fct.KeyEmployee
        , CAST(fct.CallStartTime AS DATE) CallDate
        --, fct.DialedPhoneNumber
        --, fct.DialedAreaCode
        --, fct.SessionId	
		--, fct.PhonePosition
		--, fct.zip
		--, fct.zip_scf
        , fct.contact_flag
        --, fct.CallSeconds
		, cust.ClientId
        --, CASE 
        --    WHEN dss.SourceSystem = 'Amex Latitude' 
        --        THEN ISNULL(fct.Consumer_ID, CAST(fct.KeyCustomer AS VARCHAR))
        --    ELSE CAST(fct.KeyCustomer AS VARCHAR)
        --  END AS KeyCustomer
        --, cust.CustomerId
        --, dss.KeySourceSystem
        , dss.SourceSystem
        --, dt.WeekId

        -- State flags
        , CASE WHEN zs.CustomerState = 'MA' THEN 1 ELSE 0 END AS custstate_ma
        , CASE WHEN zs.CustomerState = 'OR' THEN 1 ELSE 0 END AS custstate_or
        , CASE WHEN zs.CustomerState = 'WA' THEN 1 ELSE 0 END AS custstate_wa
        , CASE WHEN nyz.zip IS NOT NULL THEN 1 ELSE 0 END AS custstate_ny
        , CASE WHEN zs.CustomerState = 'WV' THEN 1 ELSE 0 END AS custstate_wv
        , CASE WHEN zs.CustomerState = 'DC' THEN 1 ELSE 0 END AS custstate_dc

        -- Phone type flags (config-driven)
        , CASE WHEN pt.PhoneType = 'HOME' or pp.PhoneNumber is not null THEN 1 ELSE 0 END AS phonetype_home
        , CASE WHEN pt.PhoneType = 'POE' or pp.PhoneNumber is not null THEN 1 ELSE 0 END AS phonetype_poe
        , CASE WHEN pt.PhoneType IS NULL and pp.PhoneNumber is null and fct.PhonePosition IS NOT NULL THEN 1 ELSE 0 END AS phonetype_other

        --, de.EmployeeId
        , dcl.ClientParent
        , rc.service_id
        , rc.call_center_id
		--9/23/22 - Brett adds veldos services, clientids to ignore for NY, WA, MA
        -- Suppression flag (config-driven)
        , CASE 
            WHEN rc.lv_client_name = 'Veldos'
                 AND (css.service_id IS NOT NULL 
                      OR ccsv.client_id IS NOT NULL)
            THEN 1 ELSE 0 
          END AS ny_wa_ma_suppress_flag

       -- , pp.PhoneNumber
		,count(DISTINCT fct.KeyCustomerCall) AS count_of_recs
INTO #calls										
 FROM #temp_fcc fct
    INNER JOIN #temp_rc rc
        ON fct.SessionId = rc.Session_Id
        --AND rc.Call_Date BETWEEN ISNULL(@StartDateTime, GETDATE()) - 31 AND ISNULL(@StartDateTime, GETDATE()) 
		--AND fct.KeyDate_CallDate BETWEEN CONVERT(VARCHAR,CAST(ISNULL(@StartDateTime, GETDATE()) - 31 AS DATE),112) AND CONVERT(VARCHAR,CAST(ISNULL(@StartDateTime, GETDATE()) AS DATE),112) 

    INNER JOIN DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK)
        ON fct.KeySourceSystem = dss.KeySourceSystem

    INNER JOIN DW_MSTR_DM.dbo.DimCustomer cust WITH (NOLOCK)
        ON fct.KeyCustomer = cust.KeyCustomer
        AND cust.StatusCode <> 'DW_deactivate'

    INNER JOIN DW_MSTR_DM.dbo.DimDate dt WITH (NOLOCK)
        ON fct.KeyDate_CallDate = dt.KeyDate
/*
    LEFT JOIN DW_MSTR_DM.dbo.RadiusPhone RP WITH (NOLOCK)
        ON cust.KeyCustomer = RP.KeyCustomer
        AND fct.DialedPhoneNumber = RP.PhoneNumber
*/
    LEFT JOIN DW_MSTR_DM.dbo.PhonePositions pp WITH (NOLOCK) 
	   ON pp.PhoneNumber = fct.DialedPhoneNumber 
	   AND pp.PhonePosition IN ('HOME','POE') 
	   AND pp.SourceSystem = dss.SourceSystem2  

    LEFT JOIN DW_RETENTION.dbo.zip_scf zs
        ON fct.zip_scf = zs.scf

    LEFT JOIN DW_MSTR_DM.dbo.DimEmployee de WITH (NOLOCK)
        ON fct.KeyEmployee = de.KeyEmployee

    LEFT JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK)
        ON cust.ClientId = dcl.ClientId
        AND cust.SourceSystem = dcl.SourceSystem

    LEFT JOIN DW_RETENTION.dbo.zipcodes nyz
        ON fct.zip = nyz.zip AND nyz.customerstate = 'NY'

    -- Config joins
    LEFT JOIN DW_RETENTION.dbo.Dial_Excpt_PhoneType pt
        ON fct.PhonePosition = pt.PhonePosition

    LEFT JOIN DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress css
        ON rc.service_id = css.service_id AND css.Is_Suppress = 1

    LEFT JOIN DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress ccsv
        ON dcl.ClientId = ccsv.client_id AND ccsv.Is_Suppress = 1

    LEFT JOIN DW_RETENTION.dbo.Dial_Excpt_State_Rules csr
        ON zs.CustomerState = csr.state_code
/*
    LEFT JOIN DW_RETENTION.dbo.Dial_Excpt_LiveVox_Rules lvr
        ON rc.LV_Client_Name = lvr.lv_client_name
        AND rc.Livevox_Result = lvr.livevox_result
*/
    WHERE 
        --fct.IsOutbound = 1
        --AND fct.KeyCustomer > 0

        -- Remove Amex 1st party
        --AND 
		(dss.SourceSystem <> 'Amex Latitude' OR cust.ClientId NOT LIKE 'RA%')
/*
        -- SMS suppression
        AND rc.service_name NOT LIKE '%HTI%'
        AND rc.service_name <> 'IDL Artiva SMS'
        AND rc.livevox_result NOT LIKE 'SMS%'
        AND rc.livevox_result NOT LIKE '%Text%'
*/
        -- Client exclusions 
        and cust.ClientId not in('SNBCEP','QNC01P')
/*
        -- LiveVox rules (config-driven)
		AND (
            -- If rule exists and is excluded → filter out
             lvr.is_excluded IS NULL
        )
*/
        -- State rules (config-driven)
        AND (
            csr.allow_flag = 1								 --allow_flag is 1 for zs.CustomerState in('MA','WA','NY','WV','DC')
            OR (csr.poe_only = 1 AND pt.PhoneType = 'POE')   --right now, poe_only flag is 1 only for csr.state_code = 'OR'
        )--;
 GROUP BY

 --fct.KeyCustomerCall
		--, fct.Consumer_ID  
		--,
		 fct.KeySourceSystem
		, fct.KeyDate_CallDate
        --, fct.KeyEmployee
        , CAST(fct.CallStartTime AS DATE) --CallDate
        --, fct.DialedPhoneNumber
        --, fct.DialedAreaCode
        --, fct.SessionId	
		--, fct.PhonePosition
		--, fct.zip
		--, fct.zip_scf
        , fct.contact_flag
        --, fct.CallSeconds
		, cust.ClientId
        --, CASE 
        --    WHEN dss.SourceSystem = 'Amex Latitude' 
        --        THEN ISNULL(fct.Consumer_ID, CAST(fct.KeyCustomer AS VARCHAR))
        --    ELSE CAST(fct.KeyCustomer AS VARCHAR)
        --  END AS KeyCustomer
        --, cust.CustomerId
        --, dss.KeySourceSystem
        , dss.SourceSystem
        --, dt.WeekId

        -- State flags
        , CASE WHEN zs.CustomerState = 'MA' THEN 1 ELSE 0 END --AS custstate_ma
        , CASE WHEN zs.CustomerState = 'OR' THEN 1 ELSE 0 END --AS custstate_or
        , CASE WHEN zs.CustomerState = 'WA' THEN 1 ELSE 0 END --AS custstate_wa
        , CASE WHEN nyz.zip IS NOT NULL THEN 1 ELSE 0 END --AS custstate_ny
        , CASE WHEN zs.CustomerState = 'WV' THEN 1 ELSE 0 END --AS custstate_wv
        , CASE WHEN zs.CustomerState = 'DC' THEN 1 ELSE 0 END --AS custstate_dc

        -- Phone type flags (config-driven)
        , CASE WHEN pt.PhoneType = 'HOME' or pp.PhoneNumber is not null THEN 1 ELSE 0 END --AS phonetype_home
        , CASE WHEN pt.PhoneType = 'POE' or pp.PhoneNumber is not null THEN 1 ELSE 0 END --AS phonetype_poe
        , CASE WHEN pt.PhoneType IS NULL and pp.PhoneNumber is null and fct.PhonePosition IS NOT NULL THEN 1 ELSE 0 END --AS phonetype_other

        --, de.EmployeeId
        , dcl.ClientParent
        , rc.service_id
        , rc.call_center_id
		--9/23/22 - Brett adds veldos services, clientids to ignore for NY, WA, MA
        -- Suppression flag (config-driven)
        , CASE 
            WHEN rc.lv_client_name = 'Veldos'
                 AND (css.service_id IS NOT NULL 
                      OR ccsv.client_id IS NOT NULL)
            THEN 1 ELSE 0 
          END --AS ny_wa_ma_suppress_flag

       -- , pp.PhoneNumber



	--EXCEPTIONS CALL ATTEMPTS
	--identify call attempts for each exception rule		

	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlf_expt_id=1:  MA - 2 attempts per 7 sliding for all phones
		SELECT 1 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
        INTO #exceptions
		FROM #calls
		WHERE 
		----(
		custstate_ma=1 
		----AND phonetype_home=1)
		  and ny_wa_ma_suppress_flag=0

		UNION

		--dlf_expt_id=2:  MA - 1 attempts per 30 sliding for POE
		SELECT 2 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		FROM #calls
		WHERE (custstate_ma=1 AND phonetype_poe=1)
			  and ny_wa_ma_suppress_flag=0

		UNION

		--dlf_expt_id=3:  MA - 2 attempts per 30 sliding for non-home, non-POE
		SELECT 3 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		FROM #calls
		WHERE (custstate_ma=1 AND phonetype_other=1)
			  and ny_wa_ma_suppress_flag=0

		UNION

		--dlf_expt_id=4:  OR - 1 attempts per 30 sliding for POE
		SELECT 4 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		FROM #calls
		WHERE custstate_or=1 AND phonetype_poe=1

		UNION

		--dlf_expt_id=5:  WA - 3 attempts per 7 sliding
		SELECT 5 AS dlr_excpt_id
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , c.calldate
			   , c.ClientParent
			   , c.count_of_recs
		FROM #calls c
				LEFT JOIN
             DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce ON c.ClientId = cvce.client_id AND cvce.Is_Exclude = 1
			----1stparty excluded per BrettL
				LEFT JOIN
			 DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs ON c.Service_Id = dvs.Service_Id AND dvs.Is_Exclude = 1
		WHERE custstate_wa=1 
			  			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  AND cvce.client_id IS NULL 
			  AND dvs.Service_Id IS NULL
			  --AND service_id NOT IN (110176,110175,110177,110180)            --1stparty excluded per BrettL 5/24/21
			  AND call_center_id not in (8421, 10976)  --8421 1stparty excluded per BrettL 6/1/21
													   -- 10976 1stparty excluded per BrettL on 9/6/24
			  
			  and ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=6:  NY - 2 attempts per 7 sliding
		SELECT 6 AS dlr_excpt_id
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , c.calldate
			   , c.ClientParent
			   , c.count_of_recs
		FROM #calls c
				LEFT JOIN
             DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce ON c.ClientId = cvce.client_id AND cvce.Is_Exclude = 1
			----1stparty excluded per BrettL
				LEFT JOIN
			 DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs ON c.Service_Id = dvs.Service_Id AND dvs.Is_Exclude = 1
		WHERE custstate_ny=1 			  
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  AND cvce.client_id IS NULL 
			  AND dvs.Service_Id IS NULL			    
			  --AND service_id NOT IN (110176,110175,110177,110180)            --1stparty excluded per BrettL 5/24/21		  
			  	AND call_center_id not in (8421, 10976)  --8421 1stparty excluded per BrettL 6/1/21
														 -- 10976 1stparty excluded per BrettL on 9/6/24
			  
			    and ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=7:  WV - 2 contacts per calendar week
		SELECT 7 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		FROM #calls
		WHERE custstate_wv=1 AND contact_flag=1


		UNION

		--dlf_expt_id=8:  WV - 10 attempts per calendar week
		SELECT 8 AS dlr_excpt_id
			   , clientid
			   , keysourcesystem
			   , sourcesystem
			   , calldate
			   , ClientParent
			   , count_of_recs
		FROM #calls
		WHERE custstate_wv=1 


		UNION

		--dlf_expt_id=29:  DC - 3 attempts per 7 sliding
		SELECT 29 AS dlr_excpt_id
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , c.calldate
			   , c.ClientParent
			   , c.count_of_recs
		FROM #calls c
				LEFT JOIN
             DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce ON c.ClientId = cvce.client_id AND cvce.Is_Exclude = 1
			----1stparty excluded per BrettL
				LEFT JOIN
			 DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs ON c.Service_Id = dvs.Service_Id AND dvs.Is_Exclude = 1
		WHERE custstate_dc=1 
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  AND cvce.client_id IS NULL 
			  AND dvs.Service_Id IS NULL
			  --AND service_id NOT IN (110176,110175,110177,110180)            --1stparty excluded per BrettL 5/24/21
			  AND call_center_id not in (8421, 10976)  --8421 1stparty excluded per BrettL 6/1/21
													   -- 10976 1stparty excluded per BrettL on 9/6/24
			  
			  ---and ny_wa_ma_suppress_flag=0                   -- not applicable for DC




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
