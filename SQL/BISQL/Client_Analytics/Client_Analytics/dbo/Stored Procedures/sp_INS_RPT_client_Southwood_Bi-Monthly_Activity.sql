USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_INS_RPT_client_Southwood_Bi-Monthly_Activity]    Script Date: 10/10/2024 8:14:30 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO









CREATE PROCEDURE [dbo].[sp_INS_RPT_client_Southwood_Bi-Monthly_Activity]


AS 
 

BEGIN

DECLARE 
@avFeedName VARCHAR (200) =  'Southwood_Activity_Report_' + FORMAT(getdate(), 'MMddyy'),
@dtFeedDate DATETIME = GETDATE(),
@vTemplate	VARCHAR(150) = '\\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Template\Southwood_Activity_Report_new.xls',
@vSQL		VARCHAR(8000),
@vSubject	VARCHAR(500) = @@SERVERNAME + '.' + DB_NAME() + '.dbo.' + OBJECT_NAME(@@PROCID), 
@vFile		VARCHAR(255)
 



declare @vBiMonthly	VARCHAR(100) = Replace(@avFeedName,'.csv','.xls') ;
select @vFile = '\\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Work\' + @vBiMonthly + '.xls'
-----------------------------------------------------------------------------------------------------------------------------------------------------------------

 
--Extracting Artiva CRM Data to get original account number
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1
SELECT ARACID AS CustomerID, ZZACORIGCREDACNUM as ORNGLACC, ZZACORIGCREDNM as OriginalCreditor, ARACCLACCT AS ClientAccountNumber
INTO dbo.#Results1
FROM OPENQUERY([THIRDPROD], 'SELECT ARACID, ZZACORIGCREDACNUM, ZZACORIGCREDNM  , ARACCLACCT
FROM SQLUser.ARACCOUNT WHERE ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')

--SELECT * FROM #Results1
--SELECT * INTO CLIENT_ANALYTICS.dbo.CM_STHWODRes1 FROM dbo.#Results1

--Extracting Artiva CRM Data to get Letter/Email flag
IF OBJECT_ID('tempdb.dbo.#Results_Mailflag') IS NOT NULL 
DROP TABLE dbo.#Results_Mailflag
SELECT DISTINCT a.*
INTO dbo.#Results_Mailflag
FROM OPENQUERY([THIRDPROD],
                       'SELECT  
                                    account.ARACID                           CUSTOMER_ID
                                    ,account.ARACCLTID                        CLIENT_ID
                                    ,letterhistory.ARLHLTR                    LETTER_ID
                                    ,letterhistory.ARLHPRTDTE                 FILE_DATE
                                    ,letterhistory.ARLHPRTDTE                 MAIL_DATE
									,letterhistory.ZZLTRHISEMAILSEND          MAIL_FLAG
                                    ,letterhistory.ARLHADR                    LTR_ADDRESS1
                                    ,letterhistory.ARLHCTY                    LTR_CITY
                                    ,letterhistory.ARLHST                     LTR_STATE
                                    ,letterhistory.ARLHZIP                    LTR_ZIP
									 FROM ARACCOUNT account
                                   INNER JOIN ARLTRHIS                letterhistory        ON letterhistory.ARLHACID    = account.ARACID AND ARLHSTATUS IN (''UPD'', ''MIG'')
								   WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'') AND letterhistory.ARLHLTR  like ''IDN%''
									')a

IF OBJECT_ID('tempdb.dbo.#Results_Mailflag_refined') IS NOT NULL 
DROP TABLE dbo.#Results_Mailflag_refined
SELECT customer_id
,CLIENT_ID
,LETTER_ID
,MAIL_DATE
,STRING_AGG(Mail_flag, ',') AS All_Mail_Flags 
INTO #Results_Mailflag_refined
from  #Results_Mailflag
GROUP BY customer_id
 ,CLIENT_ID
 ,LETTER_ID
 ,MAIL_DATE;

 UPDATE #Results_Mailflag_refined 
 SET All_Mail_Flags = 'P'
 WHERE
    All_Mail_Flags not like '%E%' 
 OR All_Mail_Flags NOT LIKE '%P%';
-----------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb.dbo.#prev_t1') IS NOT NULL 
DROP TABLE dbo.#prev_t1
SELECT DISTINCT CustomerId
			   --,RelationshipId
			   ,OriginalCreditor
			   ,SCRADate
			   ,ActionCode
			   ,ActionCodeDate
			   ,ActionCodeDesc
INTO #prev_t1
FROM
(
SELECT ARACID as CustomerId
	  ,ARACRPID as RelationshipId
	  ,ZZACORIGCREDNM as OriginalCreditor
	  ,ZZENFIRSCRBDTE as SCRADate
	  ,ARACTHACTID as ActionCode
	  ,ARACTHDTE as ActionCodeDate
	  ,ARACTHTIME as ActionCodeTime
	  ,ARACTDESC as ActionCodeDesc
	  ,row_number() OVER (PARTITION BY ARACID ORDER BY ARACTHDTE DESC,ARACTHTIME desc) RNK1
FROM OPENQUERY([THIRDPROD], 'SELECT account.ARACID
                            ,account.ARACRPID
                            ,account.ZZACORIGCREDNM
                            ,arentity.ZZENFIRSCRBDTE
                            ,actcode.ARACTHDTE
                            ,actcode.ARACTHACTID
							,actcode.ARACTHTIME
                            ,actcodecd.ARACTDESC
                    FROM ARCLIENT clientinfo
                    INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
                    INNER JOIN SQLUser.ARRELATIONSHIP ARREL ON account.ARACID = ARREL.ARRELACID
                    INNER JOIN SQLUser.ARENTITY arentity ON arentity.ARENID = ARREL.ARRELENID
					LEFT JOIN SQLUser.ARACTIVITYHIST actcode ON account.ARACID = actcode.ARACTHACCTID
                    LEFT JOIN SQLUser.ARACTIVITYCD actcodecd ON actcode.ARACTHACTID = actcodecd.ARACTID
                    WHERE account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')) a
WHERE A.rnk1=1
ORDER BY CustomerId,ActionCodeDate DESC

--SELECT * INTO CLIENT_ANALYTICS.dbo.CM_STHWODprev_t1 FROM dbo.#prev_t1
------------------------------------------------------------------------------------------------------------------------------
--Extracting Artiva CRM Data to get original account number
IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
DROP TABLE dbo.#Results2
SELECT ARACID AS CustomerID
	  ,row_number() over (order by ARACID) rnk1
INTO dbo.#Results2
FROM OPENQUERY([THIRDPROD], 'SELECT ARACID
FROM SQLUser.ARACCOUNT WHERE ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')')
 
IF OBJECT_ID('tempdb.dbo.#SWACCSTATUS') IS NOT NULL 
DROP TABLE dbo.#SWACCSTATUS
SELECT ARACID as CustomerId
					   ,STAUDFLDEXTNEW as StatusNew
					   ,CAST(MAX(STAUDDATE) as Date) as StatusDate
				INTO dbo.#SWACCSTATUS
				FROM OPENQUERY([THIRDPROD], 
						'SELECT account.ARACID
                                ,stchng.STAUDFLDEXTNEW
                                ,stchng.STAUDDATE
                         FROM ARCLIENT clientinfo
                              INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
                              INNER JOIN SQLUser.ARACCTAUD aduck ON account.ARACID = aduck.ARAUDACCTID       
                              INNER JOIN SQLUser.STFIELDAUD stchng ON aduck.ARAUDAUDID = stchng.STAUDID
                         WHERE LOWER(stchng.STAUDFLDDESC) = ''status''
							  AND account.ARACCLTID IN (''STHWOD'', ''STHWD2'', ''STHWD3'', ''STHWD4'', ''STHWOS'')
                              AND account.ARACID = 0000')
			GROUP BY ARACID
					,STAUDFLDEXTNEW
 
 
--No of rows in the table
DECLARE @totcnt VARCHAR (MAX);
SELECT @totcnt=COUNT(CustomerID) FROM dbo.#Results2
PRINT @totcnt
 
-- Declare variables for loop control
DECLARE @Counter INT = 1;
PRINT @Counter
 
DECLARE @AccNumb NVARCHAR(MAX);
DECLARE @SQLQuery NVARCHAR(MAX);
 
-- Start the loop
WHILE @Counter <= @totcnt
BEGIN
 
-- Select the CustomerId based on the current counter
SELECT @AccNumb = CustomerId FROM dbo.#Results2 WHERE rnk1 = @Counter;
PRINT @AccNumb;
 
DECLARE @OutputTable TABLE (
    CustomerId NVARCHAR(MAX),
    StatusNew VARCHAR(255),
    StatusDate DATETIME
);
 
SET @SQLQuery = 'SELECT ARACID as CustomerId
					   ,STAUDFLDEXTNEW as StatusNew
					   ,CAST(MAX(STAUDDATE) as Date) as StatusDate
				FROM OPENQUERY([THIRDPROD], 
						''SELECT account.ARACID
                                ,stchng.STAUDFLDEXTNEW
                                ,stchng.STAUDDATE
                         FROM ARCLIENT clientinfo
                              INNER JOIN SQLUser.ARACCOUNT account ON clientinfo.ARCLID = account.ARACCLTID
                              INNER JOIN SQLUser.ARACCTAUD aduck ON account.ARACID = aduck.ARAUDACCTID       
                              INNER JOIN SQLUser.STFIELDAUD stchng ON aduck.ARAUDAUDID = stchng.STAUDID
                         WHERE LOWER(stchng.STAUDFLDDESC) = ''''status''''
							  AND account.ARACCLTID IN (''''STHWOD'''', ''''STHWD2'''', ''''STHWD3'''', ''''STHWD4'''', ''''STHWOS'''')
                              AND account.ARACID = ' + @AccNumb + ''')
			GROUP BY ARACID
					,STAUDFLDEXTNEW';
--Print @SQLQuery;
INSERT INTO @OutputTable
EXEC sp_executesql @SQLQuery;
 
INSERT INTO dbo.#SWACCSTATUS
SELECT CustomerId,StatusNew,StatusDate
FROM @OutputTable;
 
DELETE FROM @OutputTable;
 
SET @Counter = @Counter + 1;
END;
--------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb.dbo.#prev_t1_1') IS NOT NULL 
DROP TABLE dbo.#prev_t1_1
SELECT CustomerId
	,OriginalCreditor
	,ActionCode
	,ActionCodeDesc 
	,MAX(SCRADate) as SCRADate
	,MAX(ActionCodeDate) as ActionCodeDate
INTO dbo.#prev_t1_1
FROm dbo.#prev_t1
Group By CustomerId
	,OriginalCreditor
	,ActionCode
	,ActionCodeDesc

 
-------------------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb.dbo.#t_max') IS NOT NULL 
DROP TABLE dbo.#t_max
SELECT   t.CustomerId
		,MAX(t.StatusDate) StatusDate
		into dbo.#t_max
		from dbo.#SWACCSTATUS t
		where t.StatusNew = 'LEXIS'
		group by 
		 t.CustomerId
-------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb.dbo.#t') IS NOT NULL 
DROP TABLE dbo.#t
SELECT p.CustomerId
	  --, p.RelationshipId
	  , p.OriginalCreditor
	  , p.SCRADate
	  , p.ActionCode
	  , p.ActionCodeDate
	  , p.ActionCodeDesc
	  , s.StatusNew
	  , CAST(s.StatusDate as Date) StatusDate
INTO dbo.#t
 FROM  #prev_t1 p
LEFT JOIN dbo.#SWACCSTATUS s
on s.customerid = p.customerid



IF OBJECT_ID('tempdb..#act_prev') IS NOT NULL
DROP TABLE #act_prev
SELECT 
  a.CustomerId
, a.ORNGLACC
, dcu.keycustomer
, a.ClientAccountNumber
, dcu.StatusCode
, dsc.StatusCodeDescription
, dcu.ListDate
, dcu.InitialBalance
, dcu.PaidOnAccountAmt
, dcu.IsAccountWorked
, dcu.CurrentBalance
, Concat(dcu.FirstName ,' ', dcu.LastName) AS Debtor
, dcu.CustomerState AS Debtor_State
, Concat(dcm.ComakerFirstName, ' ', dcm.ComakerLastName) AS CoDebtor
, dcm.ComakerState AS CoDebtor_State
, dcu.IDL_Date AS Debtor_MVN_Letter_Date
, CASE WHEN dcm.keycustomer IS NOT NULL THEN dcu.IDL_Date ELSE NULL END AS CoDebtor_MVN_Letter_Date
, a.OriginalCreditor
, tm.SCRADate
, tm.ActionCode
, tm.ActionCodeDate
, tm.ActionCodeDesc
, tmx.statusdate AS [Date Deceased Scrub Performed]
, tmx.statusdate AS [Date Bnk Scrub Performed]
, t.StatusDate
, rm.All_Mail_Flags AS MVN_Letter_Type
, rm.LETTER_ID AS  MVN_Letter_ID
 INTO #act_prev
FROM #Results1 a 
        LEFT JOIN DW_MSTR_DM.dbo.DimCustomer dcu WITH (NOLOCK) ON a.CustomerID = dcu.customerid
        LEFT JOIN DW_MSTR_DM.dbo.dimcomaker dcm WITH (NOLOCK) ON dcu.keycustomer = dcm.keycustomer
		LEFT JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) ON dcu.ClientId = dcl.ClientId AND dcu.sourcesystem = dcl.sourcesystem
        left join DW_MSTR_DM.dbo.DimStatusCode dsc WITH (NOLOCK) on dcu.StatusCode = dsc.StatusCode AND dsc.SourceSystem = 'THIRDPROD'
		left JOIN dbo.#prev_t1_1 tm ON dcu.CustomerId = tm.CustomerId
		left JOIN dbo.#t_max tmx ON dcu.CustomerId = tmx.CustomerId 
		LEFT join dbo.#SWACCSTATUS t ON dcu.CustomerId = t.CustomerId AND dcu.statuscode = t.StatusNew
		LEFT JOIN #Results_Mailflag_refined rm on rm.customer_id = dcu.CustomerId  AND rm.Mail_DATE = dcu.IDL_Date

where dcl.ClientId  IN ('STHWOD', 'STHWD2', 'STHWD3', 'STHWD4', 'STHWOS') 
AND dcu.keysourcesystem = 2
--AND dcu.ListDate <= CAST(GETDATE() -1 AS DATE)              
AND dcu.ListDate != '12/31/9999'
--AND (dcu.CancelDate >= CAST(GETDATE() -1  AS DATE)   OR dcu.CancelDate IS NULL)
--AND dcu.StatusCode <> 'DW_deactivate'

---------All Calls for Southwood
IF OBJECT_ID('tempdb..#calls_raw') IS NOT NULL
DROP TABLE #calls_raw;		
	SELECT    a.customerid
			, a.Keycustomer
			, ocb.ORNGLACC
			, rc.Session_Id
			, rc.Is_RPC 
			, rc.call_date
			, rc.Call_Connect_Time_CT
			, rc.livevox_result
			, rc.Service_Id
			, rc.[Service_Name]
			
	INTO #calls_raw 
	FROM  dbo.#act_prev a  
		left join  DW_MSTR_DM.dbo.RadiusCall rc WITH (NOLOCK) on rc.Account_Number = CAST(a.customerID AS VARCHAR(MAX)) 
		left join dbo.#Results1 ocb ON  rc.Account_Number=cast(ocb.CustomerID as varchar(max))
  WHERE rc.[Service_Id] IN (152014,152272,152015,152016,152017,152018,152271)
  AND ocb.ORNGLACC IS NOT NULL
-------------------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#calls_summary') IS NOT NULL
DROP TABLE #calls_summary;	
     SELECT   a.customerid
			, a.Keycustomer
			, a.ORNGLACC
			, COUNT(a.session_id) AS Total_calls
			, min(a.Call_Connect_Time_CT) AS First_Call_Attempt
			, max(a.Call_Connect_Time_CT) AS Last_Call_Attempt
			, COUNT(CASE WHEN a.Is_RPC=1 THEN 1 END) AS rpc_calls
			, MIN(CASE WHEN a.Is_RPC=1 THEN a.Call_Connect_Time_CT END) First_RPC
			, MAX(CASE WHEN a.Is_RPC=1 THEN a.Call_Connect_Time_CT END) Last_RPC
	INTO #calls_summary
	FROM #calls_raw a 
	WHERE  a.livevox_result NOT LIKE 'SMS%' and a.livevox_result NOT LIKE '%Text%' 
	GROUP BY   a.customerid
			, a.Keycustomer
			, a.ORNGLACC
----------------------------------------------------------------------------------------------------------------------------------------------

    --SMS
IF OBJECT_ID('tempdb..#sms_summary') IS NOT NULL
DROP TABLE #sms_summary;	
     SELECT   a.customerid
			, a.Keycustomer
			, max(a.Call_Connect_Time_CT) AS Last_SMS_sent
			into #sms_summary
			FROM #calls_raw a 
	WHERE  a.livevox_result  LIKE 'SMS%' OR a.livevox_result  LIKE '%Text%'
	GROUP BY   a.customerid
			, a.Keycustomer

	-- Emails
IF OBJECT_ID('tempdb..#email_summary') IS NOT NULL
DROP TABLE #email_summary;
	  SELECT  a.customerid
			, a.Keycustomer
			, MAX(rem.EventDate) AS Last_Email_sent
	INTO #email_summary
	FROM #act_prev a
				JOIN DW_MSTR_DM.dbo.Radius_EmailReportData rem WITH (NOLOCK) ON a.KeyCustomer =  rem.KeyCustomer
			WHERE  rem.[EventValue]='EMAIL_SENT'
	GROUP BY  a.customerid
			, a.Keycustomer

-- letters
IF OBJECT_ID('tempdb..#letter_summary') IS NOT NULL
DROP TABLE #letter_summary;
	SELECT    a.customerid
			, a.Keycustomer
			, CONVERT(VARCHAR,MAX(fcl.KeyDate_MailDate),112) AS Last_Letter_sent
	INTO #letter_summary
	FROM #act_prev a
			JOIN DW_MSTR_DM.dbo.FactCustomerLetter fcl WITH (NOLOCK) ON a.KeyCustomer =   fcl.KeyCustomer	
	GROUP BY  a.customerid
			, a.Keycustomer

------------------------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#Last_Worked_Date') IS NOT NULL
DROP TABLE #Last_Worked_Date;
 SELECT CustomerId, keycustomer, MAX(Last_Call_Attempt) AS Last_Worked_Date
 INTO #Last_Worked_Date
 FROM (
 SELECT cs.CustomerId, cs.keycustomer, cs.Last_Call_Attempt
 FROM #calls_summary cs
 union all
 SELECT ss.CustomerId, ss.keycustomer, ss.Last_SMS_Sent
 FROM #sms_summary ss
  union all
 SELECT es.CustomerId, es.keycustomer, es.Last_Email_Sent
 FROM #email_summary es
   union all
 SELECT ls.CustomerId, ls.keycustomer, ls.Last_Letter_Sent
 FROM #letter_summary ls
 ) combo_tables
 GROUP BY CustomerId, keycustomer
-------------------------------------------------------------------------------------------------------------------------------------
 IF OBJECT_ID('tempdb..#Worked') IS NOT NULL
DROP TABLE #Worked;			
     SELECT   a.customerid
			, a.Keycustomer
			, a.ORNGLACC
			, a.ClientAccountNumber
			, a.StatusCode
			, a.StatusCodeDescription
			, a.ListDate
			, a.InitialBalance
			, a.PaidOnAccountAmt			
			, a.IsAccountWorked
			, a.Debtor
			, a.Debtor_State
			, a.CoDebtor
			, a.CoDebtor_State
			, a.Debtor_MVN_Letter_Date
			, a.CoDebtor_MVN_Letter_Date
			, a.MVN_Letter_Type
			, a.OriginalCreditor
            , a.SCRADate
			, lwd.Last_Worked_Date
            , a.ActionCode
            , a.ActionCodeDate
            , a.ActionCodeDesc
            , a.[Date Deceased Scrub Performed]
            , a.[Date Bnk Scrub Performed]
            , a.StatusDate
			, cs.Total_calls
			, cs.First_Call_Attempt
			, cs.Last_Call_Attempt
			, cs.rpc_calls
			, cs.First_RPC
			, cs.Last_RPC 
			INTO #Worked
			FROM #act_prev a LEFT JOIN 
				 #calls_summary cs on a.KeyCustomer=cs.KeyCustomer
			LEFT JOIN #Last_Worked_Date lwd
            on cs.keycustomer = lwd.keycustomer

-------------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#Promises') IS NOT NULL
DROP TABLE #Promises;			
 SELECT       wo.customerid
			, wo.Keycustomer
			, wo.ORNGLACC
			, wo.ClientAccountNumber
			, wo.StatusCode
			, wo.StatusCodeDescription
			, wo.ListDate
			, wo.InitialBalance
			, wo.PaidOnAccountAmt			
			, wo.IsAccountWorked
			, wo.Debtor
			, wo.Debtor_State
			, wo.CoDebtor
			, wo.CoDebtor_State
			, wo.Debtor_MVN_Letter_Date
			, wo.CoDebtor_MVN_Letter_Date
			, wo.MVN_Letter_Type
			, wo.OriginalCreditor
            , wo.SCRADate
			, wo.Last_Worked_Date
            , wo.ActionCode
            , wo.ActionCodeDate
            , wo.ActionCodeDesc
            , wo.[Date Deceased Scrub Performed]
            , wo.[Date Bnk Scrub Performed]
            , wo.StatusDate
			, wo.Total_calls
			, wo.First_Call_Attempt
			, wo.Last_Call_Attempt
			, wo.rpc_calls
			, wo.First_RPC
			, wo.Last_RPC         
            , SUM(fcpd.PromiseDueAmt) AS [Periodic Payment Amount]
INTO #Promises
 FROM #Worked wo
 left join DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) 
            ON wo.KeyCustomer = fcpd.KeyCustomer
			AND CONVERT(CHAR(10), CONVERT(datetime, 
			CAST(fcpd.KeyDate_PromiseDueDate AS VARCHAR)), 120) >= CAST(GETDATE() AS DATE)		
		    AND 
		    fcpd.Date_Broken is null

			--WHERE 
			--CONVERT(CHAR(10), CONVERT(datetime, CAST(fcpd.KeyDate_PromiseDueDate AS VARCHAR)), 120) >= CAST(GETDATE() AS DATE)		
		    --AND 
		    --fcpd.Date_Broken is null
			GROUP BY 
			  wo.customerid
			, wo.Keycustomer
			, wo.ORNGLACC
			, wo.ClientAccountNumber
			, wo.StatusCode
			, wo.StatusCodeDescription
			, wo.ListDate
			, wo.InitialBalance
			, wo.PaidOnAccountAmt			
			, wo.IsAccountWorked
			, wo.Debtor
			, wo.Debtor_State
			, wo.CoDebtor
			, wo.CoDebtor_State
			, wo.Debtor_MVN_Letter_Date
			, wo.CoDebtor_MVN_Letter_Date
			, wo.MVN_Letter_Type
			, wo.OriginalCreditor
            , wo.SCRADate
			, wo.Last_Worked_Date
            , wo.ActionCode
            , wo.ActionCodeDate
            , wo.ActionCodeDesc
            , wo.[Date Deceased Scrub Performed]
            , wo.[Date Bnk Scrub Performed]
            , wo.StatusDate
			, wo.Total_calls
			, wo.First_Call_Attempt
			, wo.Last_Call_Attempt
			, wo.rpc_calls
			, wo.First_RPC
			, wo.Last_RPC 

-------------------------------------------------------------------------------------------------------------------------------------
--payment
	IF OBJECT_ID('tempdb..#pay') IS NOT NULL
		DROP TABLE #pay;

	   SELECT p.customerid
			, p.Keycustomer
			, p.ORNGLACC
			, p.ClientAccountNumber
			, p.StatusCode
			, p.StatusCodeDescription
			, p.ListDate
			, p.InitialBalance
			, p.PaidOnAccountAmt			
			, p.IsAccountWorked
			, p.Debtor
			, p.Debtor_State
			, p.CoDebtor
			, p.CoDebtor_State
			, p.Debtor_MVN_Letter_Date
			, p.CoDebtor_MVN_Letter_Date
			, p.MVN_Letter_Type
			, p.OriginalCreditor
            , p.SCRADate
			, p.Last_Worked_Date
            , p.ActionCode
            , p.ActionCodeDate
            , p.ActionCodeDesc
            , p.[Date Deceased Scrub Performed]
            , p.[Date Bnk Scrub Performed]
            , p.StatusDate
			, p.Total_calls
			, p.First_Call_Attempt
			, p.Last_Call_Attempt
			, p.rpc_calls
			, p.First_RPC
			, p.Last_RPC
			, p.[Periodic Payment Amount]
			, SUM(fcp.PaymentAppliedAmt) AS Total_Paid_to_Date
	INTO #pay
	FROM #Promises p
		left JOIN DW_MSTR_DM.dbo.FactCustomerPayment fcp WITH (NOLOCK) ON p.KeyCustomer = fcp.KeyCustomer
		left JOIN DW_MSTR_DM.dbo.DimPaymentType dpt WITH (NOLOCK)  ON fcp.KeyPaymentType = dpt.KeyPaymentType
										   AND (dpt.PaymentCategory != 'Adjustment' OR dpt.PaymentCategory IS NULL)
										   AND dpt.PaymentType NOT IN ('DA','DAR')
	 
	GROUP BY p.customerid
			, p.Keycustomer
			, p.ORNGLACC
			, p.ClientAccountNumber
			, p.StatusCode
			, p.StatusCodeDescription
			, p.ListDate
			, p.InitialBalance
			, p.PaidOnAccountAmt			
			, p.IsAccountWorked
			, p.Debtor
			, p.Debtor_State
			, p.CoDebtor
			, p.CoDebtor_State
			, p.Debtor_MVN_Letter_Date
			, p.CoDebtor_MVN_Letter_Date
			, p.MVN_Letter_Type
			, p.OriginalCreditor
            , p.SCRADate
			, p.Last_Worked_Date
            , p.ActionCode
            , p.ActionCodeDate
            , p.ActionCodeDesc
            , p.[Date Deceased Scrub Performed]
            , p.[Date Bnk Scrub Performed]
            , p.StatusDate
			, p.Total_calls
			, p.First_Call_Attempt
			, p.Last_Call_Attempt
			, p.rpc_calls
			, p.First_RPC
			, p.Last_RPC
			, p.[Periodic Payment Amount]

-------------------------------------------------------------------------------------------------------------------------------------
	IF OBJECT_ID('tempdb..#final') IS NOT NULL
		DROP TABLE #final;
	SELECT 
		customerid AS [Account Number]
	--, Keycustomer
	, OriginalCreditor AS [Original Creditor]
	--, ORNGLACC AS [Original Creditor Account Number]
	, ClientAccountNumber AS [Client Account Number]
	, '`' + CAST(ORNGLACC as varchar(255))  AS [Original Creditor Account Number]
	, StatusCode AS [Current Status]
	, StatusCodeDescription AS [Status Description]
	, Statusdate AS [Status Date]
	, ListDate AS [Placement Date]
	, InitialBalance AS [Placement Balance]			
	--, IsAccountWorked
	, Debtor_State AS [Debtor State]
	, CoDebtor_State AS [CoDebtor State]
	, SCRADate AS [SCRA Date]
    , [Date Deceased Scrub Performed] AS [Date Deceased Scrub Performed]
    , [Date Bnk Scrub Performed] AS [Date Bnk Scrub Performed]
	, Debtor_MVN_Letter_Date AS [Debtor MVN Letter date]
	, CoDebtor_MVN_Letter_Date AS [Codebtor MVN Letter date]	
    , MVN_Letter_Type AS [MVN Letter Type]
	, First_Call_Attempt AS [First Call Attempt]
	, First_RPC AS [First RPC]
	, [Periodic Payment Amount] AS [Periodic Payment Amount]
	, PaidOnAccountAmt AS [Total Paid To Date]
	--, [Total_Paid_to_Date] AS [Total Paid To Date]
	, Debtor  AS [Debtor]
	, CoDebtor AS [CoDebtor]
	, Last_Worked_Date AS [Last Worked Date]
    , ActionCode AS [Last Action Code]
	, ActionCodeDesc AS [Last Action Description]
    , ActionCodeDate AS [Last Action Date]
	, Last_Call_Attempt AS [Last Call]
	, Total_calls AS [Total Calls]
	, Last_RPC AS [Last RPC]
	, rpc_calls AS [Number Of RPC]
	INTO #final
	FROM #Pay

	 
--SELECT * FROM #final

/*
Pending metrics from the request,
[Promised Amount]

*/


TRUNCATE TABLE CLIENT_ANALYTICS.dbo.[RPT_client_Southwood_Bi-Monthly_Activity]
	

	INSERT INTO CLIENT_ANALYTICS.dbo.[RPT_client_Southwood_Bi-Monthly_Activity]
	(
		[Account Number]
		, [Original Creditor]
		, [Client Account Number]
		, [Original Creditor Account Number]
		, [Current Status]
		, [Status Description]
		, [Status Date]
		, [Placement Date]
		, [Placement Balance]			
		, [Debtor State]
		, [CoDebtor State]
		, [SCRA Date]
		, [Date Deceased Scrub Performed]
		, [Date Bnk Scrub Performed]
		, [Debtor MVN Letter date]
		, [Codebtor MVN Letter date]
		, [MVN Letter Type]		
		, [First Call Attempt]
		, [First RPC]
		, [Periodic Payment Amount]
		, [Total Paid To Date]
		, [Debtor]
		, [CoDebtor]
		, [Last Worked Date]
		, [Last Action Code]
		, [Last Action Description]
		, [Last Action Date]
		, [Last Call]
		, [Total Calls]
		, [Last RPC]
		, [Number Of RPC]
    )
	SELECT 
		[Account Number]
		, [Original Creditor]
		, [Client Account Number]
		, [Original Creditor Account Number]
		, [Current Status]
		, [Status Description]
		, [Status Date]
		, [Placement Date]
		, [Placement Balance]			
		, [Debtor State]
		, [CoDebtor State]
		, [SCRA Date]
		, [Date Deceased Scrub Performed]
		, [Date Bnk Scrub Performed]
		, [Debtor MVN Letter date]
		, [Codebtor MVN Letter date]	
		, [MVN Letter Type]	
		, [First Call Attempt]
		, [First RPC]
		, [Periodic Payment Amount]
		, [Total Paid To Date]
		, [Debtor]
		, [CoDebtor]
		, [Last Worked Date]
		, [Last Action Code]
		, [Last Action Description]
		, [Last Action Date]
		, [Last Call]
		, [Total Calls]
		, [Last RPC]
		, [Number Of RPC]
	FROM #final 



DECLARE @tab char(1) = CHAR(9)
	DECLARE @subject VARCHAR (MAX);
	DECLARE @body VARCHAR (MAX);
	SET @subject = 'Southwood Bi-Monthly Activity Report';
 
	SET @body = 'Hi All,
	Please find attached the Southwood Bi-Monthly Activity Report.
 
	Regards,
	Business Analytics';
	PRINT @body

	
SET @vSQL = 'copy /Y ' + @vTemplate + ' \\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Work\' + @vBiMonthly + '.xls';
      print @vSQL
	  EXEC xp_cmdshell @vSQL, no_output;
	
	      SET @vSQL = 'INSERT INTO OPENROWSET(''Microsoft.ACE.OLEDB.12.0'',''Excel 12.0;Database=\\DFW2-BISQL-001\SSISFlatFileStage_Offshore\Exports\Southwood\Work\' 
	  + @vBiMonthly + '.xls;'',''SELECT * FROM [Southwood_Activity_Report$]'') SELECT 
		      ISNULL([Account Number],'''')  ''Account Number''
			, ISNULL([Original Creditor],'''')  ''Original Creditor''
			, ISNULL([Client Account Number],'''')  ''Southwood Account Number''
			, ISNULL([Original Creditor Account Number],'''')  ''Original Creditor Account Number''
			, ISNULL([Current Status],'''')  ''Current Status''
			, ISNULL([Status Description],'''')  ''Status Description''
			, ISNULL(FORMAT([Status Date], ''MM/dd/yyyy'', ''en-US''),'''') ''Status Date''
			, ISNULL(FORMAT([Placement Date], ''MM/dd/yyyy'', ''en-US''),'''') ''Placement Date''
			, ISNULL(FORMAT(ROUND(ISNULL([Placement Balance], 0), 2), ''C2'', ''en-US''),'''')  ''Placement Balance''
			, ISNULL([Debtor State],'''') ''Debtor State'' 
			, ISNULL([CoDebtor State],'''')  ''CoDebtor State''
			, ISNULL(FORMAT([SCRA Date], ''MM/dd/yyyy'', ''en-US''),'''')  ''SCRA Date''
            , ISNULL(FORMAT([Date Deceased Scrub Performed], ''MM/dd/yyyy'', ''en-US''),'''')  ''Date Deceased Scrub Performed''
            , ISNULL(FORMAT([Date Bnk Scrub Performed], ''MM/dd/yyyy'', ''en-US''),'''')  ''Date Bnk Scrub Performed''
			, ISNULL(FORMAT([Debtor MVN Letter date], ''MM/dd/yyyy'', ''en-US''),'''')  ''Debtor MVN Letter date''
			, ISNULL(FORMAT([Codebtor MVN Letter date], ''MM/dd/yyyy'', ''en-US''),'''')  ''Codebtor MVN Letter date''
			, ISNULL([MVN Letter Type],'''')  ''MVN Letter Type''
			, ISNULL(FORMAT([First Call Attempt], ''MM/dd/yyyy HH:mm'', ''en-US''),'''')  ''First Call Attempt''
			, ISNULL(FORMAT([First RPC], ''MM/dd/yyyy HH:mm'', ''en-US''),'''')  ''First RPC''
			, ISNULL(FORMAT(ROUND(ISNULL([Periodic Payment Amount], 0), 2), ''C2'', ''en-US''),'''')  ''Periodic Payment Amount''
			, ISNULL(FORMAT(ROUND(ISNULL([Total Paid To Date], 0), 2), ''C2'', ''en-US''),'''')  ''Total Paid To Date''
			, ISNULL([Debtor],'''')  ''Debtor''
			, ISNULL([CoDebtor],'''')  ''CoDebtor''
			, ISNULL(FORMAT([Last Worked Date], ''MM/dd/yyyy HH:mm'', ''en-US''),'''')  ''Last Worked Date''
            , ISNULL([Last Action Code],'''')  ''Last Action Code''
			, ISNULL([Last Action Description],'''')  ''Last Action Description''
            , ISNULL([Last Action Date],'''')  ''Last Action Date''
			, ISNULL(FORMAT([Last Call], ''MM/dd/yyyy HH:mm'', ''en-US''),'''')  ''Last Call''
			, ISNULL([Total Calls],'''')  ''Total Calls''
			, ISNULL(FORMAT([Last RPC], ''MM/dd/yyyy HH:mm'', ''en-US''),'''')  ''Last RPC''
			, ISNULL([Number Of RPC],'''')  ''Number Of RPC''
	  FROM [CLIENT_ANALYTICS].dbo.[RPT_client_Southwood_Bi-Monthly_Activity]'
	  PRINT @vSQL
	  EXEC (@vSQL)
	    	
		--send email
		if (SELECT count(*) FROM CLIENT_ANALYTICS.dbo.[RPT_client_Southwood_Bi-Monthly_Activity])>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='_Group - Data Warehousing <dw@radiusgs.com>',
			--@recipients='amod.ramugade@radiusgs.com',
			
			@recipients = 'Stanley.Martin@radiusgs.com;Patsy.Delvecchio@radiusgs.com
			;Angie.Huie@radiusgs.com;jeremiah.reichert@radiusgs.com;ClientServices-All@radiusgs.com',
			-- THIS DISTRIBUTION LIST IS FROM [Radius Daily Master Data - EXTENSION - Reporting Inventory]
			--@recipients = 'Jamie.Stamp@radiusgs.com;Stanley.Martin@radiusgs.com;Andrea.Ewing@radiusgs.com;
			--		   Christi.Regan@radiusgs.com;jeremiah.reichert@radiusgs.com;ClientServices-All@radiusgs.com',
			@copy_recipients='ted.miller@radiusgs.com;dw@radiusgs.com',
			
			@subject = @subject,
			@body = @body,
			--@query = @query , 
			@execute_query_database='CLIENT_ANALYTICS'
			,@query_result_header=1		
		   ,@file_attachments = @vFile
		   ,@query_result_separator=@tab
		   ,@query_result_no_padding=1 
		   ,@query_result_width=32767;   
 

END;

GO


