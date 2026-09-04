
ALTER PROCEDURE [dbo].[sp_insert_fact_dial_excpt]
	 
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	
AS
/* 
Object: dbo.model_score_utility_model_id7

Description: Identify and insert dialer exceptions into fact_dial_excpt

Author			Date		Description
Mike Campbell	12/13/2019	Created
Mike Campbell	12/26/2019	Suppress NARS per Ted, add clientparent
Mike Campbell	01/03/2020	Suppress clientids: ('SNBCEP','QNC01P')
Mike Campbell	01/15/2020	Modify join to dimclient to included sourcesystem condition; remove join to radiuscall
Mike Campbell	01/17/2020	Assign customer to geography based on zip only
Mike Campbell	2/13/2020	changed date diff calcs to use rounded days instead of timestamps per Ted Miller instruction
Mike Campbell	4/2/2020	Needed to ignore dimcustomer.statuscode='DW_deactivate' records
Mike Campbell	10/1/2020	Brett per Amex suppressing several result codes from evaluation
Mike Campbell	5/24/2021	Brett added NY 2 per sliding 7 exclusion clientids and service_ids
Mike Campbell	9/28/2021	Brett added amex clientids to suppress from NY/WA evaluation
*/

BEGIN
	SET NOCOUNT ON;

	--DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

	-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
	DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
	WHERE calldate = isnull(@end ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
	AND dlr_excpt_id  IN (1,2,3,4,5,6,7,8,29)  
	AND KeySourceSystem not in (0,4) 

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
	WHERE dde.dlr_expt_id  IN (1,2,3,4,5,6,7,8,29) 
	AND dss.KeySourceSystem not in (0,4) 
	AND dde.all_client_flag = 1
	----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------
	declare 
		 @model_id  int = 7;


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
	where rc.Call_Date between isnull(@end, GETDATE()) - 31 and isnull(@end, GETDATE()) 
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

SELECT fct.KeyCustomerCall
		, fct.Consumer_ID  
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
        , fct.contact_flag
        , fct.CallSeconds
		, cust.ClientId
        , CASE 
            WHEN dss.SourceSystem = 'Amex Latitude' 
                THEN ISNULL(fct.Consumer_ID, CAST(fct.KeyCustomer AS VARCHAR))
            ELSE CAST(fct.KeyCustomer AS VARCHAR)
          END AS KeyCustomer
        , cust.CustomerId
        --, dss.KeySourceSystem
        , dss.SourceSystem
        , dt.WeekId

        -- State flags
        , CASE WHEN zs.CustomerState = 'MA' THEN 1 ELSE 0 END AS custstate_ma
        , CASE WHEN zs.CustomerState = 'OR' THEN 1 ELSE 0 END AS custstate_or
        , CASE WHEN zs.CustomerState = 'WA' THEN 1 ELSE 0 END AS custstate_wa
        , CASE WHEN nyz.zip IS NOT NULL THEN 1 ELSE 0 END AS custstate_ny
        , CASE WHEN zs.CustomerState = 'WV' THEN 1 ELSE 0 END AS custstate_wv
        , CASE WHEN zs.CustomerState = 'DC' THEN 1 ELSE 0 END AS custstate_dc

        -- Phone type flags (config-driven)
        , CASE WHEN pt.PhoneType = 'HOME' THEN 1 ELSE 0 END AS phonetype_home
        , CASE WHEN pt.PhoneType = 'POE'  THEN 1 ELSE 0 END AS phonetype_poe
        , CASE WHEN pt.PhoneType IS NULL and fct.PhonePosition IS NOT NULL THEN 1 ELSE 0 END AS phonetype_other

        , de.EmployeeId
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

        , pp.PhoneNumber

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
        );
 

	--CALL_SEQ
	--sequencing calls for identifying exceptions thresholds in next CTE
	
	---WITH call_seq
	---AS
	---(
	IF OBJECT_ID('tempdb..#call_seq') IS NOT NULL
		DROP TABLE #call_seq;
	SELECT DISTINCT * 
		, CASE WHEN custstate_ma=1 AND phonetype_home=1
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, dialedphonenumber, custstate_ma, phonetype_home ORDER BY callstarttime)
				END AS rank_ma_home
		, CASE WHEN custstate_ma=1 AND phonetype_poe=1
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, dialedphonenumber, custstate_ma, phonetype_poe ORDER BY callstarttime)
				END AS rank_ma_poe
		, CASE WHEN custstate_ma=1 AND phonetype_other=1
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, dialedphonenumber, custstate_ma, phonetype_other ORDER BY callstarttime)
				END AS rank_ma_other
		, CASE WHEN custstate_ma=1 
				THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ma ORDER BY callstarttime)
				END AS rank_ma 
			, CASE WHEN custstate_or=1 AND phonetype_poe=1
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_or, phonetype_poe ORDER BY callstarttime)
					END AS rank_or_poe
			, CASE WHEN custstate_wa=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_wa ORDER BY callstarttime)
					END AS rank_wa
			, CASE WHEN custstate_ny=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ny ORDER BY callstarttime)
					END AS rank_ny
			, CASE WHEN custstate_wv=1 AND contact_flag=1
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, weekid, custstate_wv, contact_flag ORDER BY callstarttime)
					END AS rank_wv_contacts
			, CASE WHEN custstate_wv=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, weekid, custstate_wv ORDER BY callstarttime)
					END AS rank_wv_attempts
			, CASE WHEN custstate_dc=1 
					THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, weekid, custstate_dc ORDER BY callstarttime)
					END AS rank_dc
			, DATEDIFF(DAY, prev1_call_start_time, callstarttime) interval1  
			, DATEDIFF(DAY, prev2_call_start_time, callstarttime) interval2  
			, DATEDIFF(DAY, prev3_call_start_time, callstarttime) interval3  
			, DATEDIFF(DAY, prev4_call_start_time, callstarttime) interval4  
	INTO #call_seq 
	FROM (
		SELECT *, 
			LAG (CallStartTime, 1) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev1_call_start_time, 
			LAG (CallStartTime, 2) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev2_call_start_time, 
			LAG (CallStartTime, 3) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev3_call_start_time, 
			LAG (CallStartTime, 34) OVER (PARTITION BY KeyCustomer ORDER BY CallStartTime) prev4_call_start_time 
		FROM (
		SELECT distinct * 
			FROM #calls 
			) tI ) t
	---),

	          
	--EXCEPTIONS
	--identify exceptions for each rule
	--in SQL 2008 there's no LAG function so have to join CALL_SEQ set to earlier calls based on their sequence nbr for each rule set
	
	---exceptions AS
	---(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlf_expt_id=1:  MA - 2 attempts per 7 sliding for all phones
		SELECT 1 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
        INTO #exceptions
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_ma - cs2.rank_ma = 2
							 --AND cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
			  and cs1.ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=2:  MA - 1 attempts per 30 sliding for POE
		SELECT 2 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_ma_poe-cs2.rank_ma_poe=1
							 and cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma_poe IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime) < 30	-- < 30 days diff between current call and 1 call ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
			  and cs1.ny_wa_ma_suppress_flag = 0
		UNION

		--dlf_expt_id=3:  MA - 2 attempts per 30 sliding for non-home, non-POE
		SELECT 3 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer = cs2.keycustomer
							 AND cs1.rank_ma_other - cs2.rank_ma_other = 2
							 AND cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma_other IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime) < 30	-- < 30 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 2 calls ago
			  AND cs1.ny_wa_ma_suppress_flag=0 
			  AND cs1.PhoneNumber IS NULL  
		UNION

		--dlf_expt_id=4:  OR - 1 attempts per 30 sliding for POE
		SELECT 4 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_or_poe-cs2.rank_or_poe=1
		WHERE cs1.rank_or_poe IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 1 call ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
		UNION

		--dlf_expt_id=5:  WA - 3 attempts per 7 sliding
		SELECT 5 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_wa-cs2.rank_wa=3
				LEFT JOIN
             DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce ON cs1.ClientId = cvce.client_id AND cvce.Is_Exclude = 1
			----1stparty excluded per BrettL
				LEFT JOIN
			 DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs ON cs1.Service_Id = dvs.Service_Id AND dvs.Is_Exclude = 1
		WHERE cs1.rank_wa IS NOT NULL 
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  --AND cs1.service_id NOT IN (110176,110175,110177,110180)
			  AND cvce.client_id IS NULL
			  AND dvs.Service_Id IS NULL
			  AND cs1.call_center_id NOT IN (8421, 10976)  --8421 1stparty excluded per BrettL 6/1/21
															-- 10976 1stparty excluded per BrettL on 9/6/24
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 3 calls ago
			  and cs1.ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=6:  NY - 2 attempts per 7 sliding
		SELECT 6 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_ny-cs2.rank_ny=2
				LEFT JOIN
             DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce ON cs1.ClientId = cvce.client_id AND cvce.Is_Exclude = 1
			----1stparty excluded per BrettL
				LEFT JOIN
			 DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs ON cs1.Service_Id = dvs.Service_Id AND dvs.Is_Exclude = 1
		WHERE cs1.rank_ny IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  --AND cs1.service_id NOT IN (110176,110175,110177,110180)
			  AND cvce.client_id IS NULL
			  AND dvs.Service_Id IS NULL
			  AND cs1.call_center_id not in (8421, 10976)  --8421 1stparty excluded per BrettL 6/1/21
															-- 10976 1stparty excluded per BrettL on 9/6/24
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
			  and cs1.ny_wa_ma_suppress_flag=0
		UNION

		--dlf_expt_id=7:  WV - 2 contacts per calendar week
		SELECT 7 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_wv_contacts-cs2.rank_wv_contacts=2
		WHERE cs1.rank_wv_contacts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 2 contacts
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < Calendar week not more than 2 contacts
		UNION

		--dlf_expt_id=8:  WV - 10 attempts per calendar week
		SELECT 8 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_wv_attempts-cs2.rank_wv_attempts=10
		WHERE cs1.rank_wv_attempts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 10 attempts
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < Calendar week not more than 10 attempts
		UNION

		--dlf_expt_id=29:  DC - 3 attempts per 7 sliding
		SELECT 29 AS dlr_excpt_id
			   , cs1.keycustomercall
			   , NULL AS call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , cs1.sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
							 AND cs1.rank_dc-cs2.rank_dc=3
				LEFT JOIN
             DW_RETENTION.dbo.Dial_Excpt_ClientID_Exclude_or_Suppress cvce ON cs1.ClientId = cvce.client_id AND cvce.Is_Exclude = 1
			----1stparty excluded per BrettL
				LEFT JOIN
			 DW_RETENTION.dbo.Dial_Excpt_1stParty_Services_Exclude_or_Suppress dvs ON cs1.Service_Id = dvs.Service_Id AND dvs.Is_Exclude = 1
		WHERE cs1.rank_dc IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago
			  --1stparty excluded per BrettL 5/24/21, many 3rd party added 9/28/21
			  --AND cs1.service_id NOT IN (110176,110175,110177,110180)
			  AND cvce.client_id IS NULL
			  AND dvs.Service_Id IS NULL
			  AND cs1.call_center_id NOT IN (8421, 10976)  --8421 1stparty excluded per BrettL 6/1/21
															-- 10976 1stparty excluded per BrettL on 9/6/24
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 3 calls ago
			  --and cs1.ny_wa_ma_suppress_flag=0   -- not applicable for DC




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
	INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,clientid,sourcesystem,calldate,sessionid,insert_date,
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
