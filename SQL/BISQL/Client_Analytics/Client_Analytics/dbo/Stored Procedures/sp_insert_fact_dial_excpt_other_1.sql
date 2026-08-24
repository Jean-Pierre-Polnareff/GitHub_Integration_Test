




create PROCEDURE [dbo].[sp_insert_fact_dial_excpt_other]
	
	@StartDateTime DATETIME = NULL
	
AS
/* 
Object: dbo.sp_insert_fact_dial_excpt_other

Description: Identify and insert dialer exceptions into fact_dial_excpt, other crms

Author			Date		Description
Mike Campbell	2/22/2022	Created
*/

BEGIN
	SET NOCOUNT ON;

DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_other_CRM_level_count]
WHERE calldate = @end
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id  IN (5,6,7,8) 

-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT @end AS day
       , dde.dlr_expt_id 
INTO #t 
FROM CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
WHERE dde.dlr_expt_id IN (5,6,7,8) 
AND dde.all_client_flag = 1
----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

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
			, case when zs.CustomerState='MA' then 1 else 0 end as custstate_ma
			, case when zs.CustomerState='OR' then 1 else 0 end as custstate_or
			, case when zs.CustomerState='WA' then 1 else 0 end as custstate_wa
			, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			, case when zs.CustomerState='WV' then 1 else 0 end as custstate_wv
			, case when rc.Is_RPC=1 then 1 else 0 end as contact_flag
			, rc.Agent_Logon_Id AS EmployeeId
			, rc.Call_Duration as CallSeconds
			, NULL AS ClientParent
			, rc.service_id
			, rc.call_center_id
	into #calls		
	FROM DW_MSTR_DM.dbo.RadiusCall rc (nolock) 
		     LEFT JOIN CLIENT_ANALYTICS.dbo.zip_scf zs WITH (NOLOCK)
        ON LEFT(rc.zip,3) = zs.scf
		     LEFT JOIN CLIENT_ANALYTICS.dbo.zipcodes nyz WITH (NOLOCK)
        ON rc.zip = nyz.zip AND nyz.customerstate = 'NY'		 
	WHERE rc.Call_Date > isnull(@StartDateTime,GETDATE())-31
	      AND rc.Transaction_Type='Outbound'
		  AND (CASE WHEN LEFT(rc.Phone_Dialed,3)='800' AND CAST(SUBSTRING(rc.Account_Number,5,9) AS INT)=0 THEN 1 ELSE 0 END) = 0
          --AND rc.Call_Date>='11/30/21'
		  AND LEFT(rc.Account_Number,4)='PROD'
		  AND rc.LV_Client_Name='RGS-CCS'
		  AND rc.Platform_Id='PROD'
		  AND rc.Livevox_Result NOT IN('AGENT - Busy','Busy','Invalid Phone Number');	      
		  

	--CALL_SEQ
	--sequencing calls for identifying exceptions thresholds in next CTE
	
	IF OBJECT_ID('tempdb..#call_seq') IS NOT NULL
		DROP TABLE #call_seq;

		SELECT *
/*			   , CASE WHEN custstate_ma=1 AND phonetype_home=1
					  THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ma, phonetype_home ORDER BY callstarttime)
					  END AS rank_ma_home
			   , CASE WHEN custstate_ma=1 AND phonetype_poe=1
					  THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ma, phonetype_poe ORDER BY callstarttime)
					  END AS rank_ma_poe
			   , CASE WHEN custstate_ma=1 AND phonetype_other=1
					  THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_ma, phonetype_other ORDER BY callstarttime)
					  END AS rank_ma_other
			   , CASE WHEN custstate_or=1 AND phonetype_poe=1
					  THEN ROW_NUMBER() OVER(PARTITION BY keycustomer, custstate_or, phonetype_poe ORDER BY callstarttime)
					  END AS rank_or_poe
*/
			   , CASE WHEN custstate_wa=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY CustomerId, custstate_wa ORDER BY callstarttime)
					  END AS rank_wa
			   , CASE WHEN custstate_ny=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY CustomerId, custstate_ny ORDER BY callstarttime)
					  END AS rank_ny
			   , CASE WHEN custstate_wv=1 AND contact_flag=1
					  THEN ROW_NUMBER() OVER(PARTITION BY CustomerId, weekid, custstate_wv, contact_flag ORDER BY callstarttime)
					  END AS rank_wv_contacts
			   , CASE WHEN custstate_wv=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY CustomerId, weekid, custstate_wv ORDER BY callstarttime)
					  END AS rank_wv_attempts
        INTO #call_seq
		FROM #calls
	---),


	--EXCEPTIONS
	--identify exceptions for each rule
	--in SQL 2008 there's no LAG function so have to join CALL_SEQ set to earlier calls based on their sequence nbr for each rule set
	
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlf_expt_id=1:  MA - 2 attempts per 7 sliding for home phone
		--SELECT 1 AS dlr_excpt_id
		--	   , cs1.keycustomercall
		--	   , NULL AS call_history_fact_id
		--	   , cs1.customerid
		--	   , cs1.clientid
		--	   , cs1.keysourcesystem
		--	   , cs1.sourcesystem
		--	   , CAST(cs1.callstarttime AS DATE) AS calldate
		--	   , cs1.sessionid
		--	   , GETDATE() AS insert_date
		--	   , cs1.DialedPhoneNumber
		--	   , cs1.CallStartTime
		--	   , cs1.EmployeeID
		--	   , cs1.CallSeconds
		--	   , cs1.ClientParent
  --      INTO #exceptions
		--FROM #call_seq cs1
		--		LEFT OUTER JOIN
		--	 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
		--					 AND cs1.rank_ma_home-cs2.rank_ma_home=2
		--WHERE cs1.rank_ma_home IS NOT NULL
		--	  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago
		--	  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
		--UNION

		----dlf_expt_id=2:  MA - 1 attempts per 30 sliding for POE
		--SELECT 2 AS dlr_excpt_id
		--	   , cs1.keycustomercall
		--	   , NULL AS call_history_fact_id
		--	   , cs1.customerid
		--	   , cs1.clientid
		--	   , cs1.keysourcesystem
		--	   , cs1.sourcesystem
		--	   , CAST(cs1.callstarttime AS DATE) AS calldate
		--	   , cs1.sessionid
		--	   , GETDATE() AS insert_date
		--	   , cs1.DialedPhoneNumber
		--	   , cs1.CallStartTime
		--	   , cs1.EmployeeID
		--	   , cs1.CallSeconds
		--	   , cs1.ClientParent
		--FROM #call_seq cs1
		--		LEFT OUTER JOIN
		--	 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
		--					 AND cs1.rank_ma_poe-cs2.rank_ma_poe=1
		--WHERE cs1.rank_ma_poe IS NOT NULL
		--	  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 1 call ago
		--	  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
		--UNION

		----dlf_expt_id=3:  MA - 2 attempts per 30 sliding for non-home, non-POE
		--SELECT 3 AS dlr_excpt_id
		--	   , cs1.keycustomercall
		--	   , NULL AS call_history_fact_id
		--	   , cs1.customerid
		--	   , cs1.clientid
		--	   , cs1.keysourcesystem
		--	   , cs1.sourcesystem
		--	   , CAST(cs1.callstarttime AS DATE) AS calldate
		--	   , cs1.sessionid
		--	   , GETDATE() AS insert_date
		--	   , cs1.DialedPhoneNumber
		--	   , cs1.CallStartTime
		--	   , cs1.EmployeeID
		--	   , cs1.CallSeconds
		--	   , cs1.ClientParent
		--FROM #call_seq cs1
		--		LEFT OUTER JOIN
		--	 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
		--					 AND cs1.rank_ma_other-cs2.rank_ma_other=2
		--WHERE cs1.rank_ma_other IS NOT NULL
		--	  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 2 calls ago
		--	  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 2 calls ago
		--UNION

		----dlf_expt_id=4:  OR - 1 attempts per 30 sliding for POE
		--SELECT 4 AS dlr_excpt_id
		--	   , cs1.keycustomercall
		--	   , NULL AS call_history_fact_id
		--	   , cs1.customerid
		--	   , cs1.clientid
		--	   , cs1.keysourcesystem
		--	   , cs1.sourcesystem
		--	   , CAST(cs1.callstarttime AS DATE) AS calldate
		--	   , cs1.sessionid
		--	   , GETDATE() AS insert_date
		--	   , cs1.DialedPhoneNumber
		--	   , cs1.CallStartTime
		--	   , cs1.EmployeeID
		--	   , cs1.CallSeconds
		--	   , cs1.ClientParent
		--FROM #call_seq cs1
		--		LEFT OUTER JOIN
		--	 #call_seq cs2 ON cs1.keycustomer=cs2.keycustomer
		--					 AND cs1.rank_or_poe-cs2.rank_or_poe=1
		--WHERE cs1.rank_or_poe IS NOT NULL
		--	  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 1 call ago
		--	  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
		--UNION

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
        INTO #exceptions
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.CustomerId=cs2.CustomerId
							 AND cs1.rank_wa-cs2.rank_wa=3
		WHERE cs1.rank_wa IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago

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
			 #call_seq cs2 ON cs1.CustomerId=cs2.CustomerId
							 AND cs1.rank_ny-cs2.rank_ny=2
		WHERE cs1.rank_ny IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago

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
			 #call_seq cs2 ON cs1.CustomerId=cs2.CustomerId
							 AND cs1.rank_wv_contacts-cs2.rank_wv_contacts=2
		WHERE cs1.rank_wv_contacts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 2 contacts

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
			 #call_seq cs2 ON cs1.CustomerId=cs2.CustomerId
							 AND cs1.rank_wv_attempts-cs2.rank_wv_attempts=10
		WHERE cs1.rank_wv_attempts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 10 attempts


--------------------------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count] for Yesterday----------------------------

INSERT INTO  CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_other_CRM_level_count]
SELECT calldate
,NULL AS keysourcesystem
,dlr_expt_id
,Count_of_Exceptions
,Insert_Date
FROM
(
SELECT #t.day AS calldate
,null AS keysourcesystem
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
group by CAST(exc.CallStartTime AS DATE)
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
	INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt_other(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,clientid,sourcesystem,calldate,sessionid,insert_date,
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

END;