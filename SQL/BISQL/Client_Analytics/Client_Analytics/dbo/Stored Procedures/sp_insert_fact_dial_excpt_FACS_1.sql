USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_FACS]    Script Date: 7/31/2023 9:47:21 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






CREATE  PROCEDURE [dbo].[sp_insert_fact_dial_excpt_FACS]
	
	@StartDateTime DATETIME = NULL
	

AS
/* 
Object: dbo.insert_fact_dial_excpt_FACS

Description: Identify and insert dialer exceptions from FACS portal into fact_dial_excpt

Author			Date		Description
Mike Campbell	12/17/2019	Created
Mike Campbell	2/13/2019	changed date diff calcs to use rounded days instead of timestamps per Ted Miller instruction

*/

BEGIN
	SET NOCOUNT ON;

	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = @end
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id   IN (1,3,5,6,7,8,29) 
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
WHERE dde.dlr_expt_id  IN (1,3,5,6,7,8,29) 
AND dss.KeySourceSystem = 4
AND dde.all_client_flag = 1
----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	--declare @model_id  int = 7;


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
            
			-- State flags
			, case when cust.CUSTOMER_STATE='MA' or ac.StateCode='MA' then 1 else 0 end as custstate_ma
			, case when cust.CUSTOMER_STATE='OR' or ac.StateCode='OR' then 1 else 0 end as custstate_or
			, case when cust.CUSTOMER_STATE='WA' or ac.StateCode='WA' then 1 else 0 end as custstate_wa
			, case when nyz.zip is not null then 1 else 0 end as custstate_ny
			, case when cust.CUSTOMER_STATE='WV' or ac.StateCode='WV' then 1 else 0 end as custstate_wv
			, case when cust.CUSTOMER_STATE='DC' or ac.StateCode='DC' then 1 else 0 end as custstate_dc
/*			
			, case when fct.PHONE_TYPE in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE') then 1 else 0 end as phonetype_home
			, case when fct.PHONE_TYPE in('DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE') then 1 else 0 end as phonetype_poe
			, case when fct.PHONE_TYPE not in('DBPHONE','DCPHONE','Home Phone','C HOME PHONE','DBPPHONE','DCPPHNE','DEBTOR POE','C POE PHONE')
												 and fct.PHONE_TYPE is not null 
												 and len(rtrim(fct.phone_type))>0 then 1 else 0 end as phonetype_other
*/
			-- Phone type flags (config-driven)
            , CASE WHEN pt.PhoneType = 'HOME' THEN 1 ELSE 0 END AS phonetype_home
            , CASE WHEN pt.PhoneType = 'POE'  THEN 1 ELSE 0 END AS phonetype_poe
			, CASE WHEN pt.PhoneType IS NULL and fct.PHONE_TYPE is not null and len(rtrim(fct.PHONE_TYPE))>0 then 1 else 0 end as phonetype_other
			
			, case when fct.RIGHT_PARTY_CONTACT='Y' then 1 else 0 end as contact_flag
			, fct.EMPLOYEE_ID as EmployeeID
			, fct.CALL_DURATION_SECONDS as CallSeconds
			, tcs.Parent as ClientParent
	into #calls		
	FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK)ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
			  LEFT outer JOIN 
		 CLIENT_ANALYTICS.dbo.AreaCodes ac WITH (NOLOCK) ON left(fct.CALL_OUT_PHONE_NUMBER,3) = ac.AreaCode AND ac.Is_FACS = 1
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
			  left outer join
		 CLIENT_ANALYTICS.dbo.zip_scf zs WITH (NOLOCK) on fct.zip_scf = zs.scf
		      left outer join
		 CLIENT_ANALYTICS.dbo.zipcodes nyz WITH (NOLOCK) on fct.zip=nyz.zip AND nyz.customerstate = 'NY'
		 -- Config joins
              LEFT JOIN 
		 CLIENT_ANALYTICS.dbo.Dial_Excpt_PhoneType pt WITH (NOLOCK) ON fct.PHONE_TYPE = pt.PhonePosition AND pt.Is_FACS = 1
		--     LEFT JOIN 
		--CLIENT_ANALYTICS.dbo.Dial_Excpt_State_Rules csr WITH (NOLOCK) ON zs.CustomerState = csr.state_code OR ac.StateCode = csr.state_code 
				     LEFT JOIN 
		CLIENT_ANALYTICS.dbo.Dial_Excpt_State_Rules csrZIP WITH (NOLOCK) ON zs.CustomerState = csrZIP.state_code 
				     LEFT JOIN 
		CLIENT_ANALYTICS.dbo.Dial_Excpt_State_Rules csrAREA WITH (NOLOCK) ON  ac.StateCode = csrAREA.state_code

	WHERE fct.CALL_DATE > isnull(@StartDateTime,GETDATE())-31
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND fct.CALL_TYPE <> 'IN'
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
/*
		  and (
				  (zs.customerstate in('MA','WA','NY','WV') or ac.StateCode in('MA','WA','NY','WV'))
				  or
				  (
					  (zs.customerstate ='OR' or ac.StateCode ='OR')
					  and
					  fct.PHONE_TYPE in('C POE PHONE')
				  )
			  );
*/
        -- State rules (config-driven)
        AND (
            csrZIP.allow_flag = 1	OR csrAREA.allow_flag = 1							             --allow_flag is 1 for zs.CustomerState in('MA','WA','NY','WV','DC')
            OR ( (csrZIP.poe_only = 1 OR csrAREA.poe_only = 1) AND pt.PhonePosition = 'C POE PHONE')   --right now, poe_only flag is 1 only for csr.state_code = 'OR'
        );



	--CALL_SEQ
	--sequencing calls for identifying exceptions thresholds in next CTE
	
	---WITH call_seq
	---AS
	---(
	IF OBJECT_ID('tempdb..#call_seq') IS NOT NULL
		DROP TABLE #call_seq;
		SELECT *
			   , CASE WHEN custstate_ma=1 AND phonetype_home=1
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid,dialedphonenumber, custstate_ma, phonetype_home ORDER BY callstarttime)
					  END AS rank_ma_home
			   , CASE WHEN custstate_ma=1 AND phonetype_poe=1
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid,dialedphonenumber, custstate_ma, phonetype_poe ORDER BY callstarttime)
					  END AS rank_ma_poe
			   , CASE WHEN custstate_ma=1 AND phonetype_other=1
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid,dialedphonenumber, custstate_ma, phonetype_other ORDER BY callstarttime)
					  END AS rank_ma_other
			   , CASE WHEN custstate_ma=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, custstate_ma ORDER BY callstarttime)
					  END AS rank_ma
			   , CASE WHEN custstate_or=1 AND phonetype_poe=1
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, custstate_or, phonetype_poe ORDER BY callstarttime)
					  END AS rank_or_poe
			   , CASE WHEN custstate_wa=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, custstate_wa ORDER BY callstarttime)
					  END AS rank_wa
			   , CASE WHEN custstate_ny=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, custstate_ny ORDER BY callstarttime)
					  END AS rank_ny
			   , CASE WHEN custstate_wv=1 AND contact_flag=1
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, weekid, custstate_wv, contact_flag ORDER BY callstarttime)
					  END AS rank_wv_contacts
			   , CASE WHEN custstate_wv=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, weekid, custstate_wv ORDER BY callstarttime)
					  END AS rank_wv_attempts
			   , CASE WHEN custstate_dc=1 
					  THEN ROW_NUMBER() OVER(PARTITION BY customerid, weekid, custstate_dc ORDER BY callstarttime)
					  END AS rank_dc
		INTO #call_seq
		FROM #calls
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
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		INTO #exceptions
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_ma - cs2.rank_ma = 2
		WHERE cs1.rank_ma IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime) < 7	-- < 7 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
		UNION

		--12/8/21 this exception disabled per Keith Sweatt
        
/*
		--dlf_expt_id=2:  MA - 1 attempts per 30 sliding for POE
		select 2 as dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , cast(cs1.callstarttime as date) as calldate
			   , null as sessionid
			   , GETDATE() as insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		from #call_seq cs1
				left outer join
			#call_seq cs2 on cs1.customerid=cs2.customerid
							 and cs1.rank_ma_poe-cs2.rank_ma_poe=1
							 and cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		where cs1.rank_ma_poe is not null
			  and DATEDIFF(day,cs2.callstarttime,cs1.callstarttime)<30-- < 30 days diff between current call and 1 call ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
		UNION
*/

		--dlf_expt_id=3:  MA - 2 attempts per 30 sliding for non-home, non-POE
		SELECT 3 AS dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_ma_other-cs2.rank_ma_other=2
							 and cs1.DialedPhoneNumber = cs2.DialedPhoneNumber
		WHERE cs1.rank_ma_other IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 2 calls ago
		UNION


		--12/8/21 this exception disabled per Keith Sweatt
/*
		--dlf_expt_id=4:  OR - 1 attempts per 30 sliding for POE
		select 4 as dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , cast(cs1.callstarttime as date) as calldate
			   , null as sessionid
			   , GETDATE() as insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		from #call_seq cs1
				left outer join
			 #call_seq cs2 on cs1.customerid=cs2.customerid
							 and cs1.rank_or_poe-cs2.rank_or_poe=1
		where cs1.rank_or_poe is not null
			  and DATEDIFF(day,cs2.callstarttime,cs1.callstarttime)<30	-- < 30 days diff between current call and 1 call ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<2592000	-- < 30 days diff between current call and 1 call ago
		UNION
*/

		--dlf_expt_id=5:  WA - 3 attempts per 7 sliding
		SELECT 5 AS dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_wa-cs2.rank_wa=3
		WHERE cs1.rank_wa IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 3 calls ago
		UNION

		--dlf_expt_id=6:  NY - 2 attempts per 7 sliding
		SELECT 6 AS dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_ny-cs2.rank_ny=2
		WHERE cs1.rank_ny IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 2 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 2 calls ago
		UNION

		--dlf_expt_id=7:  WV - 2 contacts per calendar week
		SELECT 7 AS dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_wv_contacts-cs2.rank_wv_contacts=2
		WHERE cs1.rank_wv_contacts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 2 contacts
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < Calendar week not more than 2 contacts
		UNION

		--dlf_expt_id=8:  WV - 10 attempts per calendar week
		SELECT 8 AS dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_wv_attempts-cs2.rank_wv_attempts=10
		WHERE cs1.rank_wv_attempts IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < Calendar week not more than 10 attempts
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < Calendar week not more than 10 attempts
        UNION
		--dlf_expt_id=29:  DC - 3 attempts per 7 sliding
		SELECT 29 AS dlr_excpt_id
			   , cs1.call_history_fact_id
			   , cs1.customerid
			   , cs1.clientid
			   , cs1.keysourcesystem
			   , cs1.sourcesystem
			   , CAST(cs1.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cs1.DialedPhoneNumber
			   , cs1.CallStartTime
			   , cs1.EmployeeID
			   , cs1.CallSeconds
			   , cs1.ClientParent
		FROM #call_seq cs1
				LEFT OUTER JOIN
			 #call_seq cs2 ON cs1.customerid=cs2.customerid
							 AND cs1.rank_dc-cs2.rank_dc=3
		WHERE cs1.rank_dc IS NOT NULL
			  AND DATEDIFF(DAY,cs2.callstarttime,cs1.callstarttime)<7	-- < 7 days diff between current call and 3 calls ago
			  --and DATEDIFF(SECOND,cs2.callstarttime,cs1.callstarttime)<604800	-- < 7 days diff between current call and 3 calls ago



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
	INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt(dlr_excpt_id,keycustomercall,call_history_fact_id,customerid,clientid,sourcesystem,calldate,sessionid,insert_date,
	                                          DialedPhoneNumber,CallStartTime,EmployeeID,CallSeconds,ClientParent)
	SELECT exc.dlr_excpt_id
		   , NULL AS keycustomercall
		   , exc.call_history_fact_id
		   , exc.customerid
		   , exc.clientid
		   , exc.sourcesystem
		   , exc.calldate
		   , NULL AS sessionid
		   , exc.insert_date
		   , exc.DialedPhoneNumber
		   , exc.CallStartTime
		   , exc.EmployeeID
		   , exc.CallSeconds
		   , exc.ClientParent
	FROM #exceptions exc
			LEFT OUTER JOIN
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt fde ON exc.call_history_fact_id=fde.call_history_fact_id
	WHERE fde.keycustomercall IS NULL 
		  AND fde.call_history_fact_id IS NULL		  
		  
END;