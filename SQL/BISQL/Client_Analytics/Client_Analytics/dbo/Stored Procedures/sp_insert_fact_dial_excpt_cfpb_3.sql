USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_fact_dial_excpt_cfpb]    Script Date: 12/13/2024 9:09:15 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




ALTER PROCEDURE [dbo].[sp_insert_fact_dial_excpt_cfpb]
	
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	

AS
/* 
Object: sp_insert_fact_dial_excpt_cfpb

Description: Identify and insert dialer exceptions for CFPB new rules into fact_dial_excpt

Author			Date		Description
Mike Campbell	05/03/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	--	DECLARE @end datetime =  DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0);

	-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------
	DELETE  FROM  CLIENT_ANALYTICS.dbo.[fact_dial_excpt_CRM_level_count]
	WHERE calldate = isnull(@end
							,DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	--AND CAST([Insert_Date] AS DATE) =   CAST(GETDATE() AS DATE)
	AND dlr_excpt_id IN (9,10) 
	AND KeySourceSystem  NOT IN (0,4)                                 ---IN (1,2,3,7,10)

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
	WHERE dde.dlr_expt_id IN (9,10) 
	AND dss.KeySourceSystem NOT IN (0,4)                                 ---IN (1,2,3,7,10)
	AND dde.all_client_flag = 1
	----------------------------------------- #calls for last 31 days ----------------------------------------------------------------------------------

	DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = '
		Veldos calls found for clientids not documented by Bilal or Susheel.
		See attached.

		Please contact analytics@radiusgs.com with questions or issues.';
	
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
	where fcc.KeyDate_CallDate between convert(varchar,cast(isnull(@end, GETDATE()) - 31 as date),112) and convert(varchar,cast(isnull(@end, GETDATE()) as date),112)
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
	where rc.Call_Date between isnull(@end, GETDATE()) - 31 and isnull(@end, GETDATE()) 
		AND rc.service_name NOT IN ('DEC_1P-DCA CTD','DEC_1P-DCA HCI','DEC_1P-DCA Inbound','DEC_1P-DCA Manual','DEC_1P-DCA QC',
										 'DEC_1P-DJO HCI','DEC_1P-DJO Inbound','DEC_1P-DJO Manual','DEC_1P-DJO QC','DEC_1P-DJO UAM',
										 'LXA_3P-ACB_DSA_CTD','LXA_3P-ACB_DSA_Inbound','LXA_3P-TD_DSA_CTD','LXA_3P-TD_DSA_Inbound','IDL Artiva SMS' )
		AND rc.service_name not like '%HTI%'
		AND rc.livevox_result  NOT LIKE 'SMS%' AND rc.livevox_result NOT LIKE '%Text%' 
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.Livevox_Result ELSE '' END 
				NOT IN ('AGENT - CUST RPC 12','AGENT - CUST RPC PTP 8','AGENT - CUST RPC PTP 9','AGENT - CUST 13',
					'AGENT - CUST 13','AGENT - Attorney Handling','AGENT - CUST 14','AGENT - CUST 8',
					'Busy','Fax','Invalid Phone Number','AGENT - CUST RPC PTP 6')
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.service_name ELSE '' end 
				NOT IN('Payment Verification Manual')
		AND CASE WHEN rc.LV_Client_Name='veldos' THEN rc.call_center_name ELSE '' end 
				NOT IN('Non Reg F')
		AND CASE WHEN rc.LV_Client_Name IN ('RGS-CCS', 'RGS-THI', 'RGS_Frontline','ISSManualDial') THEN rc.Livevox_Result ELSE '' end 
				NOT IN ('AGENT - CUST 10','AGENT - Attorney Handling','AGENT - CUST 11','AGENT - CUST 8','AGENT - CUST RPC 18',
					'Busy','Fax','Invalid Phone Number','AGENT - CUST RPC PTP 6','Busy','Invalid Phone Number') 
		AND rc.call_center_name  NOT IN ('Decorah Shop HQ 1P', 'Non Reg F 1ST Party') --- per Brett, excludes call centers Shop HQ 1P and Non Reg F 1ST Party 

	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
		DROP TABLE #calls;

	SELECT 
			fct.KeyCustomerCall
			, cust.ClientId
			, case when dss.SourceSystem='Amex Latitude' then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			       else CAST(fct.KeyCustomer AS VARCHAR)
				   end as KeyCustomer
			, cust.CustomerId 
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
	--7th previous call start time since limit of 7 calls in 7 days.  If 7th call in less than 7 days, exception.
			, LAG(fct.CallStartTime,7) OVER(PARTITION BY case when dss.SourceSystem='Amex Latitude' 
			                                                  then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			                                                  else CAST(fct.KeyCustomer AS VARCHAR)
				                                              end 
			                                ORDER BY fct.CallStartTime) AS callstarttime_lag7
	--isrpc_lag1 indicates previous call was an RPC
			, LAG(fct.IsRPC,1) OVER(PARTITION BY case when dss.SourceSystem='Amex Latitude' 
			                                          then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			                                          else CAST(fct.KeyCustomer AS VARCHAR)
				                                      end 
			                        ORDER BY fct.CallStartTime) AS isrpc_lag1
	--callstarttime_lag1 for calculating whether call following RPC is within 7 days
			, LAG(fct.CallStartTime,1) OVER(PARTITION BY case when dss.SourceSystem='Amex Latitude' 
			                                                  then isnull(fct.Consumer_ID,CAST(fct.KeyCustomer AS VARCHAR))
			                                                  else CAST(fct.KeyCustomer AS VARCHAR)
				                                              end 
			                                ORDER BY fct.CallStartTime) AS callstarttime_lag1 
			, CASE WHEN rc.LV_Client_Name='veldos' 
				AND dcl.clientid NOT IN ('R1FQM','R1JQM','R1JRM','R1J8M','R1J9M','R1F7M','R1GQM','R1LQM','R1L8M','R1L9M',
                                'R1G7M','R1KEM','R1KQM','R1K6M','R1K8M','R1FVM','R1J2M','RABMV','R1W3M','R1W5M',
                                'R1W7M','R1X3M','R1X5M','R1X7M','R1X1M','R1W1M','R1X2M','R1W2M','R201M','R2GBM',
                                'R2HDM','R3JBM','RGN0M','R1MQM','R1JZM','R15ZM','R1KWM','R1KZM','R1QZM','R1RZM',
                                'R24WM','R24ZM','R34ZM','RA15V','RA25V','RA35V','RA36V','RA4IV','RA43V','RA4UV',
                                'RA49V','RA4OV','RA46V','RLK1M','RLK3M','RLK4M','R3PGR','R3POR','R3PSR','R3PTR',
                                'R3CFM','RAFMV','RAJMV','R3JBM','R3JBM','R3JBM','111OHIR','111LHIR','111BHIR',
                                '11MLHIR','11MLHIR','11MOHIR','111OMDR','111LMDR','11MLMDR','11MLMDR','11MOMDR',
                                '111OLOR','111LLOR','11MLLOR','11MOLOR','111OBMR','111LBMR','11NLDGR','111CLBR',
                                '111CLBR','111CLBR','111CHBR','111CHBR','111CHBR','11MCLHR','11MCLLR','11MCOHR',
                                '11MCOLR','112ALLR','112MEDR','112LOWR','113HIGR','114QCCR','111REPR','111HEPR',
                                 '111WAIR','111LSPR','111LEPR','111GURR','111CHER','112ASPR','112AEPR','113ASPR',
                                '116ALLR','117ALLR','118ALLR','118MIDR','119ALLR','119MD1R','119IUDR','119MD2R',
                                '119OOSR','119MD3R','12LJ2AR','12LJ2JR','12LJ2ZR','113CFRR','113LFRR','113OFRR',
                                '113SFRR','111CDDR','11POHIR','11PLHIR','113HI1R','113HI2R','113HI3R','112HIGR',
								'121EBCA','121EBTA','121EBXA','121EBFA','121ETCA','121ETTA','121ETXA','121ETFA',
                                '121LBCA','121LBTA','121LBXA','121LBFA','1221BCA','122LTCA','12260XA','12260CA',
                                '12260FA','122LTFA','122LTXA','1221BXA','122LTTA','1221BFA','122LBCA','1221TCA',
                                '121DSPA','1221TXA','122LBXA','122LBFA','122LBTA','1221TFA','121CBCA','122OTHA',
                                '121CBTA','121CBXA','121CBFA','123LTTA','123LBTA','123D2AA','121LTCA','123LTCA', 
                                '123D2CA','123D2FA','123LBCA','123LBXA','123LTXA','123LBFA','122EJSA','121LTTA',
                                '123LTFA','121LTXA','121LTFA','12ECOXA','12EC2CA','12EC2AA','12ECOCA','12ECOFA',
                                '12EC2FA','12EC2TA','12ECOTA','121PLAA','121HBCA','121HBTA','121HBXA','121HBFA',
                                '122ACNA','12EACNA','RA1BM','RA1TM','RA1XM','RA1YM','RA2BM','RA2TM','RA2XM',
                                'RA2YM','RA3CM','RA3TM','RA3XM','RA3YM','RA4AM','RA4BM','RA4CM','RA4DM','RA4LM',
                                'RA4NM','RA4PM','RA4QM','RA4RM','RA4YM','RA5BM','RA5CM','RA5DM','RA5FM','RA5GM',
                                'RA5PM','RA5SM','RA5YM','RA6CM','RA6OM','RA6TM','RA6XM','RA6YM','RA78M','RA79M',
                                'RA7AM','RA7BM','RA7CM','RA7EM','RA7GM','RA7JM','RA7KM','RA7PM','RA7QM','RA7RM',
                                'RA7TM','RA7WM','RA7XM','RA7YM','RAIAM','RAIBM','RAICM','RAIHM','RAIKM','RAILM',
                                'RAINM','RAITM','RAPLM','RARAM','RARTM','RARXM','RARYM','RALOM','RAI9M','R1J2M',
                                'RLT1M','R2HIM','111BMDR','113AEPR','112LOWR','12260TA','121PABA','RA6FM','RA5JM',   
								'119SY5R','116SYCR','116SYTR','117SYCR','117SYTR','118SYCR','118SYTR','119SY0R',
								'119SY1R','119SY2R','119SY3R','119SY4R','116SYCR','116SYTR','117SYCR','117SYTR',
                                '118SYCR','118SYTR','119SY0R','119SY1R','119SY2R','119SY3R','119SY4R','119SY5R', 
								'116WLCR','117WLCR','118DIGR','118WLCR','119DIGR','119WL1R','119DGUR','119WL5R',
								'119DGOR','119WL3R','11NLDDR','11PCDGR','11PLDGR','11PODGR')
		then 0
		else 1 end as valid_clientid
		, rc.LV_Client_Name
		, rc.Livevox_Result
	INTO #calls  
	FROM #temp_fcc fct (NOLOCK)   
			  inner join
		 #temp_rc rc (nolock) on fct.SessionId = rc.Session_Id 
			  inner join
		 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on fct.KeySourceSystem=dss.KeySourceSystem
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fct.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
			  inner join
		 DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
			  LEFT outer JOIN 
		 DW_MSTR_DM.dbo.RadiusPhone RP (NOLOCK) on cust.KeyCustomer=RP.KeyCustomer and 	fct.DialedPhoneNumber=RP.PhoneNumber
		      left outer join
		 DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) on fct.KeyEmployee=de.KeyEmployee
		      left outer join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
		      LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.DimCustomerProduct dcp (NOLOCK) on cust.KeyCustomer=dcp.KeyCustomer 
         
	WHERE ISNULL(dcp.ProductType,'') NOT IN ('EX','SB','SM','SR','AB','HC','HP','BT','CB','CC','CP','CR','DV')
          
		AND (CASE WHEN LEFT(fct.DialedPhoneNumber,3)='800' AND cust.CustomerId=0 THEN 1 ELSE 0 END) = 0
		  
		AND dcl.clientid NOT IN  ('SNBCEP','DCMYSP','DCABBP','DCABYP','DCAD2P','DCADSP','DCAFDP','DCAFLP','DCAFSP','DCAKDP',
								'DCAOMP','DCAPDP','DCAVAP','DCCSSP','DCMYSP','SNBCE1','ATTMOB','ATRAB1')
		AND (dt.CalendarDate >= '11/30/21');

	--Send email on invalid Amex clientids
    --populate tmp tbl
	drop table if exists dw_staging.dbo.TMP_axp_unlist_clientid;

    select SourceSystem
           , ClientId
     	   , count(*) as calls
    into dw_staging.dbo.TMP_axp_unlist_clientid
    from #calls
    where valid_clientid=0
          and len(clientid)>0
    group by SourceSystem
           , ClientId;
		    
	--send email
	if (select count(*) from dw_staging.dbo.TMP_axp_unlist_clientid)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = @@SERVERNAME,--'DFW2-BISQL-001',
		@from_address ='dw@radiusgs.com',
		@recipients = 
		'dw@radiusgs.com',

		@subject = 'Veldos Non-Listed ClientIDs for Call Exception Reporting',

		@body = @body1,

		@query = 'select *
		from dw_staging.dbo.TMP_axp_unlist_clientid',

		@query_result_header=1, @attach_query_result_as_file=1;

	--CLEAN OUT INVALID AXP CLIENTID CALLS
	delete from #calls where valid_clientid=0;


	--EXCEPTIONS
	--identify exceptions for each rule

	---WITH exceptions AS
	---(
	IF OBJECT_ID('tempdb..#exceptions') IS NOT NULL
		DROP TABLE #exceptions;
		--dlr_expt_id=9:  CFPB - 7 attempts per 7 sliding
		SELECT 9 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
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
		INTO #exceptions
		FROM #calls c
		WHERE DATEDIFF(DAY,c.callstarttime_lag7,c.CallStartTime)<7

		UNION
		--dlr_expt_id=10:  CFPB - Post-RPC 0 attempts 7 days
		SELECT 10 AS dlr_excpt_id
			   , c.keycustomercall
			   , NULL AS call_history_fact_id
			   , c.customerid
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
		FROM #calls c
		       join
			 #calls c1 on c.KeyCustomer=c1.KeyCustomer 
			              and c1.CallStartTime<c.CallStartTime
			              and datediff(day,c1.callstarttime,c.callstarttime)<7
						  --and c1.IsRPC=1
						  and 
						  (
						  c1.IsRPC=1
						  OR (
						  c1.Livevox_Result = 'AGENT - CUST 3'
						  and c1.LV_Client_Name IN ('RGS-CCS', 'RGS-THI','RGS_Frontline','ISSManualDial')
						  )
						)
	---)

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

END;
GO


