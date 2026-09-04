
/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_cfpb_AMEX_client]    Script Date: 5/28/2026 2:04:18 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


/* 
Object: sp_insert_fact_dial_excpt_cfpb_AMEX_client
 
Description: Includes three condition rules for non-Legal 
	1. more than 21 attempts in 7 days (Consumer Level) 
	2. call in past 7 days for post-RPC attempt 
	3. more than 7 attempts in 7 days (Account Level)  

Author			Date		Description
Vlad Pilipets	5/28/26		Includes non-Legal 21 x 7 days & post-RPC & 7 x 7 
*/

ALTER PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb_AMEX_client]
	
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	 

AS 

 
BEGIN
	SET NOCOUNT ON;

	--	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

	-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
	DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
	WHERE calldate = isnull(@end
							,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
	AND dlr_excpt_id IN (38,39,40) 
	AND KeySourceSystem  IN (3)

	-----------------------------------------Cartesian product of dlr_expt_id and KeySourceSystem for Yesterday---------------------------------------
	DROP TABLE IF EXISTS #t;
	SELECT isnull(@end
				   ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS day
		   , dss.keysourcesystem
		   , dde.dlr_expt_id  
	INTO #t 
	FROM DW_MSTR_DM.dbo.DimSourceSystem dss 
		CROSS JOIN CLIENT_ANALYTICS.dbo.dim_dial_excpt dde
	WHERE dde.dlr_expt_id IN (38,39,40) 
		AND dss.KeySourceSystem IN (3)
		AND dde.all_client_flag = 0

----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	DROP TABLE IF EXISTS #temp_fcc 

	SELECT fcc.KeyCustomerCall 
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
	INTO #temp_fcc 
	FROM DW_MSTR_DM.dbo.FactCustomerCall fcc with (nolock)  
	WHERE fcc.KeyDate_CallDate between convert(varchar,cast(isnull(@end, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@end, GETDATE()) as date),112)
		AND fcc.IsOutbound = 1
		--AND fcc.callcentername NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party 
		AND fcc.KeySourceSystem IN (3)

	DROP TABLE IF EXISTS #temp_rc 
  
	SELECT rc.Session_Id,   
		rc.LV_Client_Name, 
		rc.Call_Date, 
		rc.Livevox_Result,  
		rc.service_name, 
		rc.call_center_name   
	INTO #temp_rc  
	FROM DW_MSTR_DM.dbo.RadiusCall rc with (nolock) 
	WHERE rc.Call_Date between isnull(@end, GETDATE()) - 31 and isnull(@end, GETDATE()) 
		AND rc.service_name not like '%HTI%'
		AND rc.service_name <> 'IDL Artiva SMS'
		AND rc.livevox_result NOT LIKE 'SMS%'
		AND rc.livevox_result NOT LIKE '%Text%'
		AND rc.Call_Center_Id NOT IN (10149,10976)
		--As per Brett on 6/15/2026, excluding the result codes used for Callbacks
		AND rc.Livevox_Result NOT IN ('AGENT - CUST RPC 12', 'AGENT - CUST 24', 'AGENT - CUST 15', 'AGENT - CUST 12')

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
		--21th attempt since limit of 21  calls in 7 days (FOR CONSUMER LEVEL). If 21th call in less than 7 days, exception.
		, LAG(fct.CallStartTime,22) OVER(PARTITION BY ISNULL(cust.ConsumerID, CAST(fct.KeyCustomer AS VARCHAR))			                                              
										ORDER BY fct.CallStartTime) AS callstarttime_lag21
		--isrpc_lag1 indicates previous call was an RPC
		, LAG(fct.IsRPC,1) OVER(PARTITION BY ISNULL(cust.ConsumerID, CAST(fct.KeyCustomer AS VARCHAR)) 
								ORDER BY fct.CallStartTime) AS isrpc_lag1
		--callstarttime_lag1 for calculating whether call following RPC is within 7 days
		, LAG(fct.CallStartTime,8) OVER(PARTITION BY fct.KeyCustomer 
										ORDER BY fct.CallStartTime) AS callstarttime_lag7 
		, rc.LV_Client_Name
		, rc.Livevox_Result
	INTO #calls  
	FROM #temp_fcc fct (NOLOCK)   
			inner join 
		#temp_rc rc (nolock) on fct.SessionId = rc.Session_Id 
			inner join 
		DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK) ON fct.KeyCustomer = cust.KeyCustomer 
														and cust.StatusCode <> 'DW_deactivate' 
			inner join DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem = dss.KeySourceSystem
			inner join (SELECT cl.KeyClient, cl.clientid  
					FROM [CLIENT_ANALYTICS].[dbo].[vw_Amex_ClientCodes_LookupTable] l 
						JOIN DW_MSTR_DM.DBO.DimClient cl WITH (NOLOCK) ON cl.ClientId = l.ClientCode 
					WHERE amex_segment != 'Amex - Legal') l on l.KeyClient = cust.KeyClient 
			inner join 
		DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
			LEFT outer JOIN 
		DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and 	fct.DialedPhoneNumber=RP.PhoneNumber
			left outer join
		DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
			left outer join
		DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		-- IF NEED TO INCLUDE REG-F
			--LEFT OUTER JOIN 
		-- DW_MSTR_DM.dbo.DimCustomerProduct dcp (NOLOCK) on cust.KeyCustomer=dcp.KeyCustomer 
         
	WHERE 
		-- IF NEED TO INCLUDE REG-F
		--ISNULL(dcp.ProductType,'') NOT IN ('EX','SB','SM','SR','AB','HC','HP','BT','CB','CC','CP','CR','DV') 
		-- AND 		
		(CASE WHEN LEFT(fct.DialedPhoneNumber,3)='800' AND cust.CustomerId=0 THEN 1 ELSE 0 END) = 0	   
		AND dcl.clientid NOT IN  ('SNBCEP','DCMYSP','DCABBP','DCABYP','DCAD2P','DCADSP','DCAFDP','DCAFLP','DCAFSP','DCAKDP',
								'DCAOMP','DCAPDP','DCAVAP','DCCSSP','DCMYSP','SNBCE1','ATTMOB','ATRAB1')
		AND dss.Keysourcesystem = 3;

 
	-- COLLECT EXCEPTIONS 
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
	DROP TABLE #exceptions;
	--dlr_expt_id = 38:  Client - Amex - 21x7 (Consumer ID)
	SELECT 38 AS dlr_excpt_id
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
	WHERE DATEDIFF(DAY,c.callstarttime_lag21,c.CallStartTime) < 7
	 	 
	UNION
	--dlr_expt_id = 39:  Client - Amex - 7 days post-RPC (Consumer)
	SELECT 39 AS dlr_excpt_id
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
		JOIN #calls c1 on c.ConsumerID = c1.ConsumerID
						-- and c.KeyCustomer=c1.KeyCustomer
						and c1.CallStartTime < c.CallStartTime
						and datediff(day,c1.callstarttime,c.callstarttime) < 7
						and 
						( c1.IsRPC = 1
							OR (
									c1.Livevox_Result = 'AGENT - CUST 3'
									and c1.LV_Client_Name IN ('RGS-CCS', 'RGS-THI','ISSManualDial')
								)
						)
		
	UNION 

	--dlr_expt_id = 40:  Client - Amex - 7 x 7 (Account) 
	SELECT 40 AS dlr_excpt_id
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
	WHERE DATEDIFF(DAY,c.callstarttime_lag7,c.CallStartTime) < 7
	
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
	) e ON CAST(e.calldate AS DATE) = #t.day
		AND e.keysourcesystem = #t.keysourcesystem
		AND e.dlr_excpt_id = #t.dlr_expt_id
	LEFT JOIN 
	(
	SELECT 
		dt.CalendarDate AS calldate
		, fct.KeySourceSystem
		, COUNT(*) AS No_of_Calls 
	FROM  DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
	JOIN DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
	WHERE dt.CalendarDate  = CAST(isnull(@end
										 ,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) 
								   AS DATE)
								   AND fct.KeySourceSystem IN (3)
	GROUP BY dt.CalendarDate  , fct.KeySourceSystem ) c ON #t.day = CAST(c.calldate AS DATE) 
		AND #t.keysourcesystem = c.KeySourceSystem

	GROUP BY 
		#t.day 
		,#t.keysourcesystem
		,#t.dlr_expt_id   
		,c.No_of_Calls
		) f
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

/*	
declare @cnt int; set @cnt = (select count(1) c 
						from fact_dial_excpt e 
							join dim_dial_excpt d on d.dlr_expt_id = e.dlr_excpt_id 
						where dlr_excpt_id in (38,39,40) 
							and cast(calldate as date) >= cast(getdate() - 1 as date) )

DECLARE @tab char(1) = CHAR(9) 

DECLARE @query VARCHAR (MAX); 
			SET @query = 'select d.dlr_excpt_description excpt, 
							customerid, 
							e.clientid, 
							sourcesystem, 
							cast(calldate as varchar) calldate, 
							sessionid, 
							DialedPhoneNumber, 
							convert(varchar, CallStartTime, 120) CallStartTime, 
							CallSeconds 
						from fact_dial_excpt e 
							join dim_dial_excpt d on d.dlr_expt_id = e.dlr_excpt_id 
						where dlr_excpt_id in (38,39,40) 
							and cast(calldate as date) >= cast(getdate() - 1 as date) ';

	DECLARE @body VARCHAR (MAX); 
			SET @body = 'Hi All,
		
	Amex specific exceptions executed for ' + convert(varchar,getdate() - 1,102)+'.
		
	Total count of exceptions are ' + cast(@cnt as varchar(5)) +'. 

	Regards,
	Business Analytics';

	DECLARE @subject VARCHAR (MAX); 
	SET @subject = 'Amex specific exceptions executed for ' + convert(varchar,getdate() - 1,102);
		
	--send email
	if (@cnt) > 0 
	begin
		declare @filename varchar(256) = 'Amex_Excpt_NewRules_' + convert(varchar,getdate() - 1,112) + '.csv'
			
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
		@from_address ='dw@radiusgs.com',
		@recipients = 'ted.miller@radiusgs.com;brett.leckerman@radiusgs.com;bilal.shaikh@radiusgs.com;Naumaan.Ghazali@radiusgs.com',
		@copy_recipients='vladislav.pilipets@radiusgs.com;Amod.Ramugade@radiusgs.com',
		@subject = @subject,
		@body = @body,
		@query = @query ,
		@execute_query_database='CLIENT_ANALYTICS',
		@query_result_header=1, @attach_query_result_as_file=1
		,@query_attachment_filename=@filename
		,@query_result_separator=@tab
		,@query_result_no_padding=1 
		,@query_result_width=32767; 
	end 
*/
END;
