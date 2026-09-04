

CREATE procedure [dbo].[sp_insert_fact_dial_excpt_compliance_check_facs]
 
@StartDateTime DATETIME = null,

@end datetime =  null

as
BEGIN
	SET NOCOUNT ON;


-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------

DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = cast(isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id   IN (14,15,16,17,18,19,20,21) 
AND KeySourceSystem = 4

----------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------

	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT cast(isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date) AS day
, dss.keysourcesystem
, dde.dlr_expt_id 
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK)
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_dial_excpt dde (NOLOCK)
WHERE dde.dlr_expt_id  IN (14,15,16,17,18,19,20,21) 
AND dss.KeySourceSystem = 4
AND dde.all_client_flag = 1


----FACS outbound Calls ------ 
	
	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;
	
	SELECT 
			fct.CALL_HISTORY_FACT_ID
			, fct.CLIENT_ID as clientid
			, fct.CUSTOMER_ID as customerid
			, 'FACS' as SourceSystem
			, CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime) as callstarttime
			, DATEPART(week,call_date) as WeekId
			, fct.CONTACT_CODE
			, fct.CALL_OUT_PHONE_NUMBER as DialedPhoneNumber
			, left(fct.CALL_OUT_PHONE_NUMBER,3) as DialedAreaCode
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

			  --callstarttime_lag3 for calculating more than 3 call attempts per day
			, LAG((CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)),3) 
			      OVER(PARTITION BY cust.CUSTOMER_ID 
				       ORDER BY (CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime))) 
			  AS callstarttime_lag3
			
              , t.State_Name
			  , CASE
                WHEN t.state_time_zone = 'AKST (UTC-09)' THEN DATEADD(hour, -3,(CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)))
                WHEN t.state_time_zone = 'CST (UTC-6)' THEN DATEADD(hour, 0,(CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)))
                WHEN t.state_time_zone = 'EST (UTC-5)' THEN DATEADD(hour, 1,(CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)))
                WHEN t.state_time_zone = 'HST (UTC-10)' THEN DATEADD(hour, -4,(CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)))
                WHEN t.state_time_zone = 'MT (UTC-07)' THEN DATEADD(hour, -1,(CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)))
                WHEN t.state_time_zone = 'PT (UTC-8)' THEN DATEADD(hour, -2,(CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime)))
                ELSE NULL
                END AS Localized_Time
				, cust.CANCEL_CODE
			, cast(cust.CANCEL_DATE as date) as CANCEL_DATE
			, cast(cust.LAST_MODIFIED_DATE as date) as LAST_MODIFIED_DATE

	into #calls		
	FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK) ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
		   LEFT OUTER JOIN 
		DW_MSTR_DM.dbo.TimeZoneByState T (NOLOCK) ON Cust.CUSTOMER_STATE = T.State_Abbr
				          
	WHERE fct.CALL_DATE >= isnull(@StartDateTime,GETDATE())-31
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND fct.CALL_TYPE <> 'IN'
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
      --11/30/21 TedM/Keith add 3 term codes to suppress
		  AND fct.TERMINATION_CODE NOT IN('3T','BU','FX')
		  and cust.CLIENT_ID not like 'DC%' and cust.CLIENT_ID not like '%P'
		and cust.CLIENT_ID not like 'EMPCMF'
		  ;


-------Insert new recs for First ever restricted calls from into build_first_ever_restricted_Calls table-----------------------------------
		  
INSERT INTO CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls 
(KeyCustomer,CustomerID, Callstarttime, insert_date, Livevox_result, SourceSystem)

----drop table if exists #facs_resrticted_calls

	   select null as KeyCustomer
	   ,xyz.CUSTOMER_ID as customerid
       ,min(CAST(xyz.CALL_DATE AS DATETIME) + CAST(xyz.call_start_time as datetime)) as callstarttime
       ,getdate() as insert_date       
       ,xyz.CONTACT_CODE
	   ,'FACS' as SourceSystem

----into #facs_resrticted_calls

	  FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT xyz (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK) ON xyz.CUSTOMER_ID = cust.CUSTOMER_ID
		 left join CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls w
		 ON  xyz.CUSTOMER_ID = w.CustomerID
		 AND  w.SourceSystem = 'FACS'
		 AND  CAST(xyz.CALL_DATE AS DATETIME) + CAST(xyz.call_start_time as datetime) >= w.callstarttime 
		 AND xyz.CONTACT_CODE = w.Livevox_Result 
       	
	WHERE 
	xyz.CALL_DATE > isnull(@StartDateTime,GETDATE())-7
		  and xyz.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND xyz.CALL_TYPE <> 'IN'
		  and ISNULL(xyz.Data_Source,'') = 'NGLV'      
	      AND xyz.CONTACT_CODE  in ('RAT', 'WPC', 'RDA')		  
		  group by xyz.CUSTOMER_ID
				   ,xyz.CONTACT_CODE

;

-------Insert new recs for First ever restricted calls from into build_first_ever_restricted_DNC_Calls table-----------------------------------
		  
INSERT INTO CLIENT_ANALYTICS.dbo.build_first_ever_restricted_DNC_Calls 
(KeyCustomer,CustomerID, Callstarttime, insert_date, Livevox_result, SourceSystem, PhoneNumber)

----drop table if exists #facs_resrticted_DNC_calls

	   select null as KeyCustomer
	   ,xyz.CUSTOMER_ID as customerid
       ,min(CAST(xyz.CALL_DATE AS DATETIME) + CAST(xyz.call_start_time as datetime)) as callstarttime
       ,getdate() as insert_date       
       ,xyz.CONTACT_CODE
	   ,'FACS' as SourceSystem
	   ,xyz.CALL_OUT_PHONE_NUMBER

----into #facs_resrticted_DNC_calls

	  FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT xyz (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK) ON xyz.CUSTOMER_ID = cust.CUSTOMER_ID
		 left join CLIENT_ANALYTICS.dbo.build_first_ever_restricted_DNC_Calls w
		 ON  xyz.CUSTOMER_ID = w.CustomerID
		 AND  w.SourceSystem = 'FACS'
		 AND  CAST(xyz.CALL_DATE AS DATETIME) + CAST(xyz.call_start_time as datetime) >= w.callstarttime 
		 AND xyz.CONTACT_CODE = w.Livevox_Result 
		        	
	WHERE 
	xyz.CALL_DATE > isnull(@StartDateTime,GETDATE())-7
		  and xyz.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  AND xyz.CALL_TYPE <> 'IN'
		  and ISNULL(xyz.Data_Source,'') = 'NGLV'      
	      AND xyz.CONTACT_CODE  = 'DNC'		  
		  group by xyz.CUSTOMER_ID
				   ,xyz.CONTACT_CODE
				   ,xyz.CALL_OUT_PHONE_NUMBER
;

------EXCEPTIONS------
	--identify exceptions for each rule
	drop table if exists #exceptions

		------dlr_expt_id=14:  not more than 3 attempts per day
				
				SELECT 14 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , Status_Changed_Date = NULL
			  
		into #exceptions

		FROM #calls c
		WHERE 
		CAST(c.callstarttime_lag3 AS DATE) = CAST(c.CallStartTime AS DATE)

		UNION

	--------dlr_expt_id=15:  Post-DNC 0 attempts for lifetime

		       SELECT 15 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   			  
		FROM #calls c 
		INNER JOIN
        (
        SELECT     
          customerid
		, PhoneNumber
        , CallStartTime
        , SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_DNC_Calls (NOLOCK)
        where Livevox_Result = 'DNC'
        ) w
        on c.CustomerId = w.customerid
		AND c.DialedPhoneNumber = w.PhoneNumber
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem = w.SourceSystem
		AND  datediff(hour, w.callstarttime, c.CallStartTime) > 24
		AND c.DialedPhoneNumber <> '0' 

		
			UNION
		----dlr_expt_id=16:  Post Attorney Handling/Representation 0 attempts for lifetime	
		SELECT 16 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   
		FROM #calls c
		INNER JOIN
        (
          SELECT     
          customerid
        , CallStartTime
        , SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'RAT'
        ) w
        on c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
		AND  datediff(hour, w.callstarttime, c.CallStartTime) > 24
		AND c.DialedPhoneNumber <> '0' 

		UNION
    ----dlr_expt_id=17:  Post Bankrupt 0 attempts for lifetime
		SELECT 17 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL			   
			   , isnull(c.cancel_date,c.last_modified_date) as Status_Changed_Date
			   
		FROM #calls c
        where left(CANCEL_CODE,2) = 'BK' 
		and isnull(c.cancel_date,c.last_modified_date) > CAST(c.callstarttime AS DATE)            
        AND c.DialedPhoneNumber <> '0'  
		AND  datediff(hour, callstarttime, c.CallStartTime) > 24


		UNION
    ----dlr_expt_id=18:  Post Deceased 0 attempts for lifetime	
		SELECT 18 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , isnull(c.cancel_date,c.last_modified_date) as Status_Changed_Date
			   
		FROM #calls c
		where c.CANCEL_CODE = 'DEC' 
		and isnull(c.cancel_date,c.last_modified_date) > CAST(c.callstarttime AS DATE)
		AND  datediff(hour, callstarttime, c.CallStartTime) > 24
		AND c.DialedPhoneNumber <> '0' 


		UNION
    ----dlr_expt_id=19:  Post Debtor Disputes 0 attempts for lifetime	
		SELECT 19 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   

		FROM #calls c
		INNER JOIN
        (
        SELECT     
          customerid
        , CallStartTime
        , SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result = 'RDA'
        ) w
        on c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
		AND  datediff(hour, w.callstarttime, c.CallStartTime) > 24
		AND c.DialedPhoneNumber <> '0'  

/*
		UNION
    ----dlr_expt_id=20:  Post WPC 0 attempts for lifetime	
		SELECT 20 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL AS sessionid
			   , GETDATE() AS insert_date
			   , cast(c.DialedPhoneNumber as varchar) as DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   

		FROM #calls c
		INNER JOIN
        (
        SELECT     
          customerid
        , CallStartTime
        , SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'WPC'
        ) w
        on c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
        AND c.DialedPhoneNumber <> '0' 
*/
			   
	UNION
    ----dlr_expt_id=21:  No calls between 8 am to 9 pm on customer local time
			
		SELECT 21 AS dlr_excpt_id
			   , NULL AS keycustomercall
			   , c.call_history_fact_id
			   , c.customerid
			   , c.clientid
			   , c.sourcesystem
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , NULL as sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , c.EmployeeID
			   , c.CallSeconds
			   , c.ClientParent
               , c.Localized_Time
               , c.State_Name
			   , Status_Changed_Date = NULL
			   		  
		FROM #calls c 	
		where datepart(hour, Localized_Time) not between 8 and 21


--------------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count] for Yesterday----------------------------

INSERT INTO  CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count]
(calldate
,keysourcesystem
,dlr_excpt_id
,Count_of_Exceptions
,Insert_Date)

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
,keysourcesystem = 4
,exc.dlr_excpt_id
,COUNT(*) AS Count_of_Exceptions
FROM #exceptions exc 
group by CAST(exc.CallStartTime AS DATE)

,exc.dlr_excpt_id
) E
ON CAST(e.calldate AS DATE) = #t.day
	AND e.keysourcesystem = #t.keysourcesystem
	AND e.dlr_excpt_id = #t.dlr_expt_id
LEFT JOIN 
(
SELECT CAST(CALL_DATE AS DATE) as calldate
, KeySourceSystem = 4 
, COUNT(*) AS No_of_Calls 
FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT (NOLOCK)
WHERE CAST(CALL_DATE AS DATE) = cast(isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
GROUP BY CAST(CALL_DATE AS DATE)
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

	
------INSERT exception data into table CLIENT_ANALYTICS.dbo.fact_dial_excpt----

INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt
(
        dlr_excpt_id
      , keycustomercall
      , call_history_fact_id
      , customerid
	  , clientid
	  , sourcesystem
	  , calldate,sessionid,insert_date
	  , DialedPhoneNumber
	  , CallStartTime
	  , EmployeeID
	  , CallSeconds
	  , ClientParent
	  , Localized_Time
	  , State_Name
	  , Status_Changed_Date
	  
	  )
	
	
	SELECT   exc.dlr_excpt_id
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
		   , exc.Localized_Time
           , exc.State_Name
		   , exc.Status_Changed_Date
		   

	      FROM #exceptions exc
LEFT OUTER JOIN
          CLIENT_ANALYTICS.dbo.fact_dial_excpt fde (NOLOCK) 
		  ON exc.call_history_fact_id=fde.call_history_fact_id
          WHERE fde.keycustomercall IS NULL
          AND fde.call_history_fact_id IS NULL 	

END;