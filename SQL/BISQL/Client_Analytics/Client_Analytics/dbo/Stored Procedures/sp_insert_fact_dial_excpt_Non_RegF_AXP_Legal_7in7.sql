
CREATE PROCEDURE [dbo].[sp_insert_fact_dial_excpt_Non_RegF_AXP_Legal_7in7]

     @StartDate DATETIME = NULL
	, @EndDate DATETIME =  NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_Non_RegF_AXP_Legal_7in7

Description: Identify and insert dialer exceptions for AXP Legal Client – Amex – Non_RegF - 7 attempts per 7 sliding rule into fact_dial_excpt

Author			Date		Description
Amod Ramugade	02/23/2026	Created
*/

BEGIN
	SET NOCOUNT ON;

	--	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

------------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
	DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
	WHERE calldate = isnull(@EndDate
							,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
	AND dlr_excpt_id IN (50) 
	AND KeySourceSystem IN (3)                                 

-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
		IF OBJECT_ID('tempdb..#t') IS NOT NULL
			DROP TABLE #t;
	SELECT isnull(@EndDate
				   ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day
		   , dss.keysourcesystem
		   , dde.dlr_expt_id 
	INTO #t 
	FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
	CROSS JOIN 
	CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
	WHERE dde.dlr_expt_id IN (50)
	AND dss.KeySourceSystem IN (3)                             
	AND dde.all_client_flag = 0
	
 DROP TABLE IF EXISTS #temp_fcc;
 	SELECT 
			fct.KeyCustomerCall
			, fct.KeyCustomer
			, fct.KeyEmployee
			, fct.CallStartTime
			, fct.DialedPhoneNumber
			, fct.DialedAreaCode
			, fct.SessionId
			, fct.CallSeconds
		
	INTO #temp_fcc 
	
	FROM DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
	where fct.KeyDate_CallDate between convert(varchar,cast(isnull(@EndDate, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@EndDate, GETDATE()) as date),112)
	AND fct.KeyCustomer > 0
	and fct.KeySourceSystem = 3
	AND fct.IsOutbound=1


	drop table if exists #temp_rc ;
  
	select
		  rc.service_name
        , rc.service_id
        , rc.Call_Center_Name
        , rc.lv_client_name
		, rc.Session_Id
        , rc.Livevox_Result
		, rc.Call_Date
			into #temp_rc  
	from DW_MSTR_DM.dbo.RadiusCall rc with (nolock) 
	where rc.Call_Date between isnull(@enddate, GETDATE()) - 31 and isnull(@enddate, GETDATE()) 
        -- SMS suppression
        AND rc.service_name NOT LIKE '%HTI%'
		AND rc.LV_Client_Name = 'Veldos'
        AND rc.livevox_result NOT LIKE 'SMS%'
        AND rc.livevox_result NOT LIKE '%Text%'
		AND rc.Call_Center_Name IN ('Non Reg F', 'Non Reg F 1st party')

--------------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------
DROP TABLE IF EXISTS #calls;
	SELECT 
			fct.KeyCustomerCall
			, fct.KeyCustomer
			, fct.KeyEmployee
			, fct.CallStartTime
			, fct.DialedPhoneNumber
			, fct.DialedAreaCode
			, fct.SessionId
			, fct.CallSeconds
		
	INTO #calls 
	
	FROM #temp_fcc fct 
	join DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK) ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
	join [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] (nolock) lup on lup.ClientCode= cust.ClientId
                                                                                    and cust.SourceSystem= 'AMEX Latitude'
																					and lup.Amex_Segment_Group = 'Legal'


---------------------------------------------------------- filtering out SMS and HTI calls ----------------------------------------------------------------------------------
	DROP TABLE IF EXISTS #calls_minus_HTI
SELECT  c.*
,rc.Call_Center_Name
,rc.service_name
, rc.Livevox_Result into #calls_minus_HTI 
FROM #calls c
	  inner join
		 #temp_rc rc
		 on c.SessionId=rc.Session_Id 
		 --AND  rc.Call_Date between isnull(@EndDate, GETDATE()) - 31 and isnull(@EndDate, GETDATE()) 
		 --AND rc.LV_Client_Name = 'Veldos'
		 --AND rc.service_name not like '%HTI%'
		 --AND rc.Livevox_Result not like '%SMS%'
	     --AND rc.Call_Center_Name IN ('Non Reg F', 'Non Reg F 1st party')

--SELECT * INTO DW_RETENTION.dbo.FCC_AXP_Legal_Non_REgF FROM #calls_minus_HTI

---------------------------------------------------------------- calculating lag7 required to check the exception rule ---------------------------------------------------
DROP TABLE IF EXISTS #calls_lag7
	SELECT *
	, LAG(fct.CallStartTime,7) OVER(PARTITION BY fct.KeyCustomer ORDER BY fct.CallStartTime) AS callstarttime_lag7
	 INTO #calls_lag7
	FROM #calls_minus_HTI fct

----------------------------------------------------------------identify exceptions for each rule --------------------------------------------------------------------------
	DROP TABLE IF EXISTS #exceptions
				------dlr_expt_id=50:  Client – Amex – Non_RegF - 7 attempts per 7 sliding
				SELECT 50 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , cust.customerid
			   , cust.clientid
			   , CAST(c.callstarttime AS DATE) AS calldate
			   , c.sessionid
			   , GETDATE() AS insert_date
			   , c.DialedPhoneNumber
			   , c.CallStartTime
			   , de.EmployeeID
			   , dcl.ClientParent
			   , dcp.ProductType
			   , c.KeyCustomer
			   , c.KeyEmployee
			   , c.DialedAreaCode
			   , c.Call_Center_Name
			   , c.Livevox_Result
			   , c.CallSeconds
			   , dss.SourceSystem

into #exceptions
	
		FROM #calls_lag7 c

 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.DimCustomerProduct dcp (NOLOCK) on c.KeyCustomer=dcp.KeyCustomer
		 join DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK) ON c.KeyCustomer = cust.KeyCustomer 
		 		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on c.KeyEmployee=de.KeyEmployee
		      left join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		 left join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on cust.KeySourceSystem=dss.KeySourceSystem
		WHERE DATEDIFF(DAY,c.callstarttime_lag7,c.CallStartTime)<7


--SELECT  *	 FROM #exceptions exc 

-- where ISNULL(exc.ProductType,'') NOT IN ('EX','SB','SM','SR'),'AB','HC','HP','BT','CB','CC','CP','CR','DV')



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
,keysourcesystem = 3
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
SELECT 
CAST(c.CallStartTime AS DATE) AS calldate
, COUNT(*) AS No_of_Calls 
FROM  #calls_minus_HTI c
WHERE CAST(c.CallStartTime AS DATE)  = CAST(isnull(@EndDate,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS DATE)
GROUP BY CAST(c.CallStartTime AS DATE)
)c
ON #t.day = CAST(c.calldate AS DATE) 
	AND #t.keysourcesystem = 3

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
		 CLIENT_ANALYTICS.dbo.fact_dial_excpt fde (NOLOCK) 
		 ON exc.keycustomercall = fde.keycustomercall AND exc.dlr_excpt_id = fde.dlr_excpt_id
	WHERE fde.keycustomercall IS NULL AND fde.dlr_excpt_id IS NULL
		  AND fde.call_history_fact_id IS NULL   

END;
