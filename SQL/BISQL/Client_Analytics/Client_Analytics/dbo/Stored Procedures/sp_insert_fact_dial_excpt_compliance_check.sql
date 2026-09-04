use CLIENT_ANALYTICS
go


alter procedure [dbo].[sp_insert_fact_dial_excpt_compliance_check]

@StartDateTime DATETIME = null,

@end datetime =  null

as 

BEGIN
	SET NOCOUNT ON;

-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------

DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
WHERE calldate = cast(isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
AND dlr_excpt_id  IN (14,15,16,17,18,19,20,21)  
AND KeySourceSystem in (1,2,3)

---------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------

	IF OBJECT_ID('tempdb..#t') IS NOT NULL
		DROP TABLE #t;
SELECT isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as day 
, dss.keysourcesystem
, dde.dlr_expt_id 
INTO #t 
FROM DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK)
CROSS JOIN 
CLIENT_ANALYTICS.dbo.dim_dial_excpt dde (NOLOCK)
WHERE 
dde.dlr_expt_id  IN (14,15,16,17,18,19,20,21) 
AND 
dss.KeySourceSystem in (1,2,3)
AND dde.all_client_flag = 1

	
-----AMEX and Artiva Outbound Calls---------

DROP TABLE	IF EXISTS  #calls

SELECT 
              fct.KeyCustomerCall
			, cust.ClientId
			, fct.KeyCustomer
			, cust.CustomerId
			, cust.CustomerState
			, fct.KeyEmployee
			, dss.SourceSystem
			, dss.KeySourceSystem
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
			, fct.IsOutbound	
			, rc.Livevox_Result
			-------To get calls for more than 3 times in a day for a customer
			,LAG(fct.CallStartTime,3) OVER(PARTITION BY cust.KeyCustomer ORDER BY fct.CallStartTime) AS callstarttime_lag3
			,t.State_Name
			,rc.Service_Name
			  
              , CASE
WHEN t.state_time_zone = 'AKST (UTC-09)' THEN DATEADD(hour, -3,fct.CallStartTime)
WHEN t.state_time_zone = 'CST (UTC-6)' THEN DATEADD(hour, 0,fct.CallStartTime)
WHEN t.state_time_zone = 'EST (UTC-5)' THEN DATEADD(hour, 1,fct.CallStartTime)
WHEN t.state_time_zone = 'HST (UTC-10)' THEN DATEADD(hour, -4,fct.CallStartTime)
WHEN t.state_time_zone = 'MT (UTC-07)' THEN DATEADD(hour, -1,fct.CallStartTime)
WHEN t.state_time_zone = 'PT (UTC-8)' THEN DATEADD(hour, -2,fct.CallStartTime)
ELSE NULL
END AS Localized_Time

			INTO #calls 
	
	FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id and rc.Call_Date >= isnull(@StartDateTime,GETDATE()) -31
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and fct.DialedPhoneNumber=RP.PhoneNumber
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.TimeZoneByState T (NOLOCK) ON cust.CustomerState = T.State_Abbr
		 left join [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup
         on lup.ClientCode= cust.ClientId
         and cust.SourceSystem= 'AMEX Latitude'
					 
	WHERE dt.CalendarDate >= isnull(@StartDateTime,GETDATE())-31 
	    AND fct.IsOutbound=1
		and (lup.FirstPartyFlag <> 1 or lup.FirstPartyFlag is NULL)
--7/5/22  -  TedM requests all services containing "HTI" (sms) to be suppressed 
		and rc.Service_Name not like '%HTI%'
		AND dcl.ClientId NOT LIKE 'DC%P' 
	    AND dcl.ClientId NOT IN ('SNBCEP', 'EMPBSP','EMPCMF','EMPIFP') 

-----INSERT first ever restricted calls INTO CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls

INSERT INTO CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls 
(KeyCustomer,CustomerID, Callstarttime, insert_date, Livevox_result, SourceSystem)

----drop table if exists #firsteverrestrictedcalls

SELECT fct.KeyCustomer
, cust.CustomerID
, MIN(fct.CallStartTime) as CallStartTime
, GETDATE() as Insert_Date 
, rc.Livevox_Result
, dss.SourceSystem

----into #firsteverrestrictedcalls

		FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id 
		 AND rc.Call_Date >= ISNULL(@StartDateTime,GETDATE())-7
		 			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer 
		 and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
		 left join 
		 [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup
         on lup.ClientCode= cust.ClientId
         and cust.SourceSystem= 'AMEX Latitude' 
		 LEFT OUTER JOIN 
		 CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK) w 
		 ON fct.KeyCustomer = w.KeyCustomer 
		 AND dss.SourceSystem = w.SourceSystem
		 AND  fct.CallStartTime >= w.callstarttime 
		 AND rc.Livevox_Result = w.Livevox_Result
		 		 
WHERE   
        dt.CalendarDate >= ISNULL(@StartDateTime,GETDATE())-7
		and fct.IsOutbound=1
		and (lup.FirstPartyFlag <> 1 or lup.FirstPartyFlag is NULL)
		and rc.Service_Name not like '%HTI%'
		AND rc.Livevox_Result  in 
		(
		'AGENT - Attorney Handling'
        ,'AGENT - Bankrupt'
        ,'AGENT - Deceased'
        ,'AGENT - Debtor Dispute'
        ,'AGENT - Wrong Number'
        )

GROUP BY 	  fct.KeyCustomer
		    , cust.CustomerId
			, rc.Livevox_Result
			, dss.SourceSystem
			
;
-------Insert new recs for First ever restricted DNC calls from past 7 days into build_first_ever_restricted_DNC_Calls table-----------------------------------

INSERT INTO CLIENT_ANALYTICS.dbo.build_first_ever_restricted_DNC_Calls 
(KeyCustomer,CustomerID, Callstarttime, insert_date, Livevox_result, SourceSystem, PhoneNumber)

----drop table if exists #firsteverrestrictedDNCcalls

SELECT  fct.KeyCustomer
      , cust.CustomerID
      , MIN(fct.CallStartTime) AS CallStartTime
      , GETDATE() as Insert_Date 
      , rc.Livevox_Result
      , dss.SourceSystem
	  , rp.PhoneNumber
	
----into #firsteverrestrictedDNCcalls

		FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
			  inner join
		 DW_MSTR_DM.dbo.RadiusCall rc (nolock) on fct.SessionId=rc.Session_Id 
		 AND rc.Call_Date >= ISNULL(@StartDateTime,GETDATE())-7
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
	
		LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer 
		 and fct.DialedPhoneNumber = RP.PhoneNumber
		 /* removed because phone no., employee name, client parent, client ID etc. not required
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		 */
		 left join [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup
         on lup.ClientCode= cust.ClientId
         and cust.SourceSystem= 'AMEX Latitude'
		 LEFT OUTER JOIN 
		 CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK) w 
		 ON fct.KeyCustomer = w.KeyCustomer 
		 AND dss.SourceSystem = w.SourceSystem
		 AND  fct.CallStartTime >= w.callstarttime 
		 AND rc.Livevox_Result = w.Livevox_Result 
		
WHERE 
        dt.CalendarDate >= ISNULL(@StartDateTime,GETDATE())-7
		and fct.IsOutbound=1
		and (lup.FirstPartyFlag <> 1 or lup.FirstPartyFlag is NULL)
		AND rc.Livevox_Result  = 'AGENT - DNC'
		---AND  datediff(hour, d.callstarttime, fct.CallStartTime) > 24

GROUP BY 	  fct.KeyCustomer
		    , cust.CustomerId
			, rc.Livevox_Result
			, dss.SourceSystem
			, rp.phonenumber
			
;

--EXCEPTIONS
	--identify exceptions for each rule

	drop table if exists #exceptions

	------dlr_expt_id=14:  not more than 3 attempts per day
	
				SELECT 14 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , Status_Changed_Date = NULL
			  			   
	    into #exceptions

		FROM #calls c
		WHERE 
		 CAST(c.callstarttime_lag3 AS DATE) = CAST(c.CallStartTime AS DATE)
----As per Brett on 12/5/22, excluded below service_name
		 and c.service_name not in (
'1st Party CTD Manual'
,'1st Party HCI'
,'1st Party Preview'
,'3rd Party CTD Manual'
,'Corporate CTD Manual'
,'Boomerang CTD Manual'
,'Boomerang HCI'
,'Boomerang Inbound Email'
,'Boomerang Inbound LCID'
,'Boomerang Inbound Letters'
,'Boomerang Inbound Messages'
,'Boomerang Inbound SMS'
,'Boomerang Preview'
,'Boomerang Restricted States Preview'
,'Boomerang TFN CTD'
,'Boomerang TFN Manual'
,'New Hire Boomerang CTD'
,'103_Primary_Lending_CTD_Manual'
,'103_Primary_Lending_HCI'
,'103_Primary_Lending_Mplus'
,'103_Primary_Lending_QC'
,'103_Primary_Lending_QC_Preview_All'
,'High Prime CTD Manual'
,'High Prime HCI'
,'High Prime Inbound Email'
,'High Prime Inbound LCID'
,'High Prime Inbound Letters'
,'High Prime Inbound Messages'
,'High Prime Inbound SMS'
,'High Prime TFN CTD'
,'High Prime TFN Manual'
,'Lending Preview'
,'DEC_1P-Shop HQ 1 HCI'
,'DEC_1P-Shop HQ 1 Manual'
,'DEC_1P-Shop HQ HCI'
,'DEC_1P-Shop HQ Letters Inbound'
,'DEC_1P-Shop HQ Manual'
,'DEC_1P-Shop HQ TFN LCID Inbound'
,'DEC_1P-Shop HQ UAM'

)
			  
	UNION
    ---dlr_expt_id=15:  Post-DNC 0 attempts for lifetime	
		
		SELECT 15 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			                	  
		FROM #calls c 
		INNER JOIN
        (
        SELECT
         keycustomer
        ,customerid
        ,CallStartTime
        ,SourceSystem
        ,PhoneNumber
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_DNC_Calls (NOLOCK)
        WHERE Livevox_Result= 'AGENT - DNC'
        ) w
        ON c.KeyCustomer = w.keycustomer
        and c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
        and c.DialedPhoneNumber = w.PhoneNumber
		AND  datediff(hour, w.callstarttime, c.CallStartTime) > 24
		AND c.DialedPhoneNumber <> 0  
			      	  
		UNION
    ----dlr_expt_id=16: Post Attorney Handling/Representation 0 attempts for lifetime	
		
		SELECT 16 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			    
		FROM #calls c 
		INNER JOIN
        (
        SELECT
         keycustomer
        ,customerid
        ,CallStartTime
        ,SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'AGENT - Attorney Handling'
        ) w
        ON c.KeyCustomer = w.keycustomer
        and c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
		AND datediff(hour, w.callstarttime, c.CallStartTime) > 24
        AND c.DialedPhoneNumber <> 0  
					  
		UNION
    ----dlr_expt_id=17:  Post Bankrupt 0 attempts for lifetime
				
		SELECT 17 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   	  
		FROM #calls c 
		INNER JOIN
        (
        SELECT
         keycustomer
        ,customerid
        ,CallStartTime
        ,SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'AGENT - Bankrupt'
        ) w
        ON c.KeyCustomer = w.keycustomer
        and c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
		AND datediff(hour, w.callstarttime, c.CallStartTime) > 24
        AND c.DialedPhoneNumber <> 0  
	  
		UNION
    ----dlr_expt_id=18: Post Deceased 0 attempts for lifetime
				
		SELECT 18 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   		  
		FROM #calls c 
		INNER JOIN
        (
        SELECT
         keycustomer
        ,customerid
        ,CallStartTime
        ,SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'AGENT - Deceased'
        ) w
        ON c.KeyCustomer = w.keycustomer
        and c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
		AND datediff(hour, w.callstarttime, c.CallStartTime) > 24
        AND c.DialedPhoneNumber <> 0 	
	  	  
		UNION
    ----dlr_expt_id=19:  Post Debtor Disputes 0 attempts for lifetime
			
		SELECT 19 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
			   	  
		FROM #calls c 
		INNER JOIN
        (
        SELECT
         keycustomer
        ,customerid
        ,CallStartTime
        ,SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'AGENT - Debtor Dispute'
        ) w
        ON c.KeyCustomer = w.keycustomer
        and c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
		AND datediff(hour, w.callstarttime, c.CallStartTime) > 24
        AND c.DialedPhoneNumber <> 0 	
  			  
-----10/12/22 removed rule 20 as too many exceptions.  will research and add later maybe.
/*
	UNION
    ----dlr_expt_id=20:  Post WPC 0 attempts for lifetime
			
		SELECT 20 AS dlr_excpt_id
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
			   , Localized_Time = NULL
			   , State_Name = NULL
			   , w.CallStartTime as Status_Changed_Date
  
		FROM #calls c 
		INNER JOIN
        (
        SELECT
         keycustomer
        ,customerid
        ,CallStartTime
        ,SourceSystem
        FROM CLIENT_ANALYTICS.dbo.build_first_ever_restricted_Calls (NOLOCK)
        WHERE Livevox_Result= 'AGENT - Wrong Number'
        ) w
        ON c.KeyCustomer = w.keycustomer
        and c.CustomerId = w.customerid
        AND c.CallStartTime > w.CallStartTime
        and c.SourceSystem= w.SourceSystem
        AND c.DialedPhoneNumber <> 0 
				AND  datediff(hour, w.callstarttime, c.CallStartTime) > 24	
*/	
		   
	UNION
    ----dlr_expt_id=21:  No calls between 8 am to 9 pm on customer local time	
			
		SELECT 21 AS dlr_excpt_id
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
               , c.Localized_Time
               , c.State_Name
			   , Status_Changed_Date = NULL
			   
		  
		FROM #calls c 	
		where datepart(hour, Localized_Time) not between 8 and 21


-----------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count] for Yesterday----------------------------

INSERT INTO CLIENT_ANALYTICS.[dbo].[fact_dial_excpt_CRM_level_count]

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
WHERE dt.CalendarDate  = cast(isnull(@end,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) as date)
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
	

---------insert exceptions data from 2022 into fct_dial_expt

INSERT INTO CLIENT_ANALYTICS.dbo.fact_dial_excpt
(
             dlr_excpt_id
		   , keycustomercall
		   , call_history_fact_id
		   , customerid
		   , clientid
		   , sourcesystem
		   , calldate
		   , sessionid
		   , insert_date
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
		LEFT JOIN
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt fde (NOLOCK) 
		 ON exc.keycustomercall = fde.keycustomercall
	     WHERE fde.keycustomercall IS NULL 
		 AND fde.call_history_fact_id IS NULL  

END;
GO