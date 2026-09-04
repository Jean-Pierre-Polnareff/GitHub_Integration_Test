




CREATE PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb_facs]
	
	@StartDateTime DATETIME = NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb_facs

Description: Identify and insert dialer exceptions for CFPB new rules into fact_dial_excpt

Author			Date		Description
Mike Campbell	10/15/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = @end
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id   IN (9,10) 
AND KeySourceSystem = 4

-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT @end AS day, dss.keysourcesystem
, dde.dlr_expt_id 
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
WHERE dde.dlr_expt_id  IN (9,10) 
AND dss.KeySourceSystem = 4
AND dde.all_client_flag = 1
----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	SELECT 
			fct.CALL_HISTORY_FACT_ID
			, fct.CLIENT_ID as clientid
			, fct.CUSTOMER_ID as customerid
			, KeySourceSystem = 4
			, 'FACS' as SourceSystem
			, CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime) as callstarttime
			, DATEPART(week,call_date) as WeekId
			, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
			--, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end as custstate_ma
			--, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end as custstate_or
			--, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end as custstate_wa
			--, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			--, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end as custstate_wv
/* removing phonetype_home, phonetype_poe and phonetype_other as it's not being used to identify the exceptions
			, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end as phonetype_home
			, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end as phonetype_poe
			, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
												 and fct.PHONE_TYPE is not null 
												 and len(rtrim(fct.phone_type))>0 then 1 else 0 end as phonetype_other
*/
			, case when fct.RIGHT_PARTY_CONTACT='Y' then 1 else 0 end as contact_flag
			, fct.EMPLOYEE_ID as EmployeeID
			, fct.CALL_DURATION_SECONDS as CallSeconds
			, tcs.Parent as ClientParent
	--7th previous call start time since limit of 7 calls in 7 days.  If 7th call in less than 7 days, exception.
			, LAG((CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)),7) 
			      OVER(PARTITION BY cust.CUSTOMER_ID 
				       ORDER BY (CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime))) 
			  AS callstarttime_lag7
/* removing isrpc_lag1 and callstarttime_lag1 as it's not being used to identify the exceptions
	--isrpc_lag1 indicates previous call was an RPC
	--12/13 - Keith says RPF/RNF contact_codes make a callback within 7 ok, so modifying logic
--			, LAG(fct.IsAdjRPC,1) 
			, LAG(case when fct.IsAdjRPC=1 and right(fct.contact_code,1)<>'F' then 1 else 0 end,1) 
			      OVER(PARTITION BY cust.CUSTOMER_ID 
				       ORDER BY (CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime))) 
			  AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG((CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)),1) 
			      OVER(PARTITION BY cust.CUSTOMER_ID 
				       ORDER BY (CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime))) 
			  AS callstarttime_lag1
*/
			, fct.isAdjRPC as IsRPC
			, fct.contact_code
	into #calls		
	FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK)ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
	WHERE fct.CALL_DATE > isnull(@StartDateTime,GETDATE())-31
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND fct.CALL_TYPE <> 'IN'
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
		 --11/30/21 TedM/Keith add 3 term codes to suppress
		  AND fct.TERMINATION_CODE NOT IN('3T','BU','FX')
/*
		 --all clients eligible for calls 11/30+
	      AND (
		       fct.CALL_DATE>='11/30/21'		  
		  --12/13/21 per BobR, remove testing period from reporting process
		  /*
		       OR
		  --checking for specific clients during specific dates
		       (
			     (fct.CALL_DATE>='10/11/21' AND fct.CLIENT_ID='WEC1')
				 OR
                 (fct.CALL_DATE>='11/1/21' AND tcs.parent='Onemain')
			   )
		  */
			  )
*/
		  ;


	--EXCEPTIONS
	--identify exceptions for each rule

	--WITH exceptions AS
	--(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlr_expt_id=9:  CFPB - 7 attempts per 7 sliding
		SELECT 9 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
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
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.keysourcesystem
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
		FROM #calls c
		       join
			 #calls c1 on c.customerid=c1.customerid
			              and c1.CallStartTime<c.CallStartTime
			              and datediff(day,c1.callstarttime,c.callstarttime)<7
						  and c1.IsRPC=1
						  and right(c1.contact_code,1)<>'F'
	----)

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
SELECT CAST(CAST(CALL_DATE AS DATETIME) + CAST(call_start_time as datetime) AS DATE ) as calldate
, KeySourceSystem = 4 
, COUNT(*) AS No_of_Calls 
FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT (NOLOCK)
WHERE CAST(CAST(CALL_DATE AS DATETIME) + CAST(call_start_time as datetime) AS DATE ) = CAST(@end AS DATE)
GROUP BY CAST(CAST(CALL_DATE AS DATETIME) + CAST(call_start_time as datetime) AS DATE )
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



----------------------------------------------------------Insert to fact_dial_excpt--------------------------------------------------------
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
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt fde (NOLOCK) ON exc.CALL_HISTORY_FACT_ID=fde.call_history_fact_id
	WHERE fde.keycustomercall IS NULL 
		  AND fde.call_history_fact_id IS NULL                            

END;