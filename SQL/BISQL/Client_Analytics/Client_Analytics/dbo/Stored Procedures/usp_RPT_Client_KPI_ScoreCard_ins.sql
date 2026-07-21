CREATE PROCEDURE [dbo].[usp_RPT_Client_KPI_ScoreCard_ins]
		

AS


BEGIN
SET NOCOUNT ON;
	 
	  DECLARE @ReportStartDate DATETIME = NULL;
      DECLARE @StartDate DATETIME 

-----------------if Start Date is not provided, default to 1st of month 12 months ago---------------------------------------------------------
SET @StartDate = (SELECT CASE WHEN @ReportStartDate IS NOT NULL THEN @ReportStartDate --WHEN @StartDate IS NOT NULL THEN @StartDate
				ELSE DATEADD(m,-12,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0)) END); 

-----------------------Main Table...All client id and month combinations------------------------------------------------------
IF OBJECT_ID('Tempdb..#ClientKPIMonths') IS NOT NULL
        DROP TABLE  #ClientKPIMonths;

    SELECT  
            cmmg.SourceSystem
		   ,cmmg.ClientStream
		   ,cmmg.ClientStreamId
		   ,cmmg.ClientParent
           ,cmmg.ClientId
           --,mth.CALNDR_DT
           --,mth.Week_ID
           ,mth.MONTH_DATE
    INTO    #ClientKPIMonths
    
    FROM    CLIENT_ANALYTICS.dbo.vw_ClientMapping_MergeGroups cmmg (NOLOCK)
    CROSS JOIN (SELECT DISTINCT 
	                   -- CALNDR_DT
                       --,Week_ID
                       MONTH_DATE
                FROM    DW_MSTR_DM.dbo.LU_DATE (NOLOCK)
                WHERE   CALNDR_DT >= @StartDate 
                        AND CALNDR_DT < GETDATE()) mth
    ORDER BY mth.MONTH_DATE ASC;

	
CREATE INDEX IX_MONTH_DATE_ClientKPIMonths
ON #ClientKPIMonths (MONTH_DATE)

CREATE INDEX IX_SourceSystem_ClientKPIMonths
ON #ClientKPIMonths (SourceSystem)

CREATE INDEX IX_ClientId_ClientKPIMonths
ON #ClientKPIMonths (ClientId)

-----------------------------------------------------------#ClientKPIPlacements - Placement Data---------------------------------------------------------------------

    IF OBJECT_ID('tempdb..#ClientKPIPlacements') IS NOT NULL
        DROP TABLE  #ClientKPIPlacements;
        
     SELECT dss.SourceSystem
	---- CASE WHEN dcs.SourceSystem = 'THIRDPROD' THEN 'ThirdProd Artiva' 
	----             WHEN dcs.SourceSystem = 'MedPROD' THEN 'MedProd Artiva' 
	----             ELSE dcs.SourceSystem 
	----			 END AS SourceSystem
            
	       ,dcs.CLIENTID
           ---,dcs.LISTDATE
		   ,dcs.BatchMonth AS LISTMONTH
           ,Placed_Ct = SUM(1) 
           ,Placed_Amt = SUM(dcs.INITIALBALANCE) 
    INTO    #ClientKPIPlacements
     FROM    DW_MSTR_DM.dbo.DimCustomer (NOLOCK) dcs
	 LEFT JOIN DW_MSTR_DM.dbo.DimSourceSystem (NOLOCK) dss
	 ON dcs.SourceSystem = dss.SourceSystem2
    WHERE   dcs.BATCHMONTH >= @StartDate
    GROUP BY dss.SourceSystem
	        ---dcs.SourceSystem
	       ,dcs.CLIENTID
           ---,dcs.LISTDATE
		   ,dcs.BatchMonth

		   
		   UNION

		    SELECT  
			SourceSystem = 'FACS'
			,AA.CLIENT_ID
			--,AA.LIST_DATE
           ,DATEADD(month, DATEDIFF(month, 0, AA.LIST_DATE), 0) AS LIST_MONTH
           ,Placed_Ct = SUM(1) 
           ,Placed_Amt = SUM(BB.INITIAL_BALANCE) 
      FROM    DW_MSTR_DM.dbo.LU_CUSTOMER AA (NOLOCK)
    JOIN    DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT BB (NOLOCK)
            ON AA.CUSTOMER_ID = BB.CUSTOMER_ID
    WHERE   AA.LIST_DATE >= @StartDate
    GROUP BY AA.CLIENT_ID
           --,AA.LIST_DATE
		   ,DATEADD(month, DATEDIFF(month, 0, AA.LIST_DATE), 0);

CREATE INDEX IX_LISTMONTH_ClientKPIPlacements
ON #ClientKPIPlacements (LISTMONTH)

CREATE INDEX IX_SourceSystem_ClientKPIPlacements
ON #ClientKPIPlacements (SourceSystem)

CREATE INDEX IX_ClientId_ClientKPIPlacements
ON #ClientKPIPlacements (ClientId)		   

-------------------------------------------------#calls_month - Calls data----------------------------------------------------------------------------------------
	  DROP TABLE IF EXISTS  #calls_month
	  	SELECT
	   calls.[crm]
      ,calls.[MONTH_DATE]
      ,calls.[CLIENT_ID]
      ,SUM(calls.[calls]) AS [calls]
	  ,SUM(CASE WHEN calls.[call_direction] = 'Outbound' THEN calls.calls END) AS  Calls_Outbound
	  ,SUM(CASE WHEN calls.[call_direction] = 'Inbound' THEN calls.calls END) AS  Calls_Inbound
	  ,SUM(CASE WHEN calls.[call_type] = 'Manual' THEN calls.calls END) AS  Calls_Manual
	  ,SUM(CASE WHEN calls.[call_type] = 'Dialer' THEN calls.calls END) AS  Calls_Dialer
	  ,SUM(calls.[rpcs]) AS [rpcs]
	  ,SUM(CASE WHEN calls.[call_direction] = 'Inbound' THEN calls.rpcs END) AS  RPCs_Inbound
	  ,SUM(CASE WHEN calls.[call_type] = 'Manual' THEN calls.rpcs END) AS  RPCs_Manual
	  ,SUM(CASE WHEN calls.[call_type] = 'Dialer' THEN calls.rpcs END) AS  RPCs_Dialer
      ,SUM(calls.[connects]) AS [connects]
	  ,SUM(CASE WHEN calls.[call_direction] = 'Inbound' THEN calls.connects END) AS  Connects_Inbound
	  ,SUM(CASE WHEN calls.[call_type] = 'Manual' THEN calls.connects END) AS  Connects_Manual
	  ,SUM(CASE WHEN calls.[call_type] = 'Dialer' THEN calls.connects END) AS  Connects_Dialer
      ,SUM(calls.[promises]) AS [promises]
	  ,SUM(CASE WHEN calls.[call_direction] = 'Inbound' THEN calls.promises END) AS  Promises_Inbound
	  ,SUM(CASE WHEN calls.[call_type] = 'Manual' THEN calls.promises END) AS  Promises_Manual
	  ,SUM(CASE WHEN calls.[call_type] = 'Dialer' THEN calls.promises END) AS  Promises_Dialer
      ,SUM(calls.[Call_Duration_Seconds]) AS [Call_Duration_Seconds]    
      ,SUM(calls.[talk_time]) AS [talk_time]
      ,SUM(calls.[AGENT_UPDATE_TIME]) AS [AGENT_UPDATE_TIME]
      ,calls.[KeySourceSystem]
	  INTO #calls_month
	  FROM [CLIENT_ANALYTICS].[dbo].[RPT_CallActivity] calls (NOLOCK)
	  WHERE calls.MONTH_DATE >= @StartDate
	  GROUP BY
	   calls.[crm]
      ,calls.[MONTH_DATE]
      ,calls.[CLIENT_ID]
      ,calls.[KeySourceSystem]

CREATE INDEX IX_Month_Date_Calls
ON #calls_month (MONTH_DATE)

CREATE INDEX IX_CRM_Calls
ON #calls_month (crm)

CREATE INDEX IX_KeySourceSystem_Calls
ON #calls_month (KeySourceSystem)

CREATE INDEX IX_ClientId_Calls
ON #calls_month (Client_Id)

--------------------------------------------------#pymt_month - Payments data--------------------------------------------------------------------------------------------
	 DROP TABLE IF EXISTS #pymt_month
	  SELECT 
	   pymt.[crm]
      ,COUNT(DISTINCT pymt.[CUSTOMER_ID])  AS Accts_worked 
	  ,DATEADD(m, DATEDIFF(m, 0, pymt.[pymt_date]), 0) AS MONTH_DATE
      ,pymt.[CLIENT_ID]
      ,SUM(pymt.[total_collections]) AS [total_collections]
      ,SUM(pymt.[total_fees]) AS [total_fees]
	  ,SUM(pymt.[total_payments]) AS [total_payments]
	  ,SUM(CASE WHEN pymt.[first_payment_flag] = 1 THEN pymt.[total_collections] END) AS  first_payment_collections
	  ,SUM(CASE WHEN pymt.[first_payment_flag] = 1 THEN pymt.[total_payments] END) AS  first_payment_collections_count    
	  INTO #pymt_month
	  FROM [CLIENT_ANALYTICS].[dbo].[RPT_payment_detail] pymt (NOLOCK)
	  WHERE pymt.pymt_date  >= @StartDate
	  GROUP BY 
	   pymt.[crm]
	  ,DATEADD(m, DATEDIFF(m, 0, pymt.[pymt_date]), 0) 
      ,pymt.[CLIENT_ID]

CREATE INDEX IX_Month_Date_Pymt
ON #pymt_month (MONTH_DATE)

CREATE INDEX IX_CRM_Pymt
ON #pymt_month (crm)

CREATE INDEX IX_ClientId_Pymt
ON #pymt_month (Client_Id)

-----------------------------------------------------------#inv - Active Inventory data---------------------------------------------------------------    
	 DROP TABLE IF EXISTS #inv
	 SELECT 
	   inv.[rpt_month]
      ,inv.[KeySourceSystem]
      ,inv.[ClientId]
      ,SUM(inv.[accounts]) AS [accounts]
  INTO #inv
  FROM [CLIENT_ANALYTICS].[dbo].[RPT_Active_Inv_Performance] inv (NOLOCK)
  WHERE inv.rpt_month  >= @StartDate
  GROUP BY 
       inv.[rpt_month]
      ,inv.[KeySourceSystem]
      ,inv.[ClientId]
    
CREATE INDEX IX_rpt_Month_Inv
ON #inv (rpt_month)

CREATE INDEX IX_KeySourceSystem_Inv
ON #inv (KeySourceSystem)

CREATE INDEX IX_ClientId_Inv
ON #inv (ClientId)
------------------------------------------------#Email_month--------------------------------------------------------------------

	DROP TABLE IF EXISTS #Email_month

	SELECT 
	 E.crm
	,DATEADD(m, DATEDIFF(m, 0, E.send_date), 0) AS MONTH_DATE
	,E.clientid
	,SUM(E.[sent]) AS Email_sent
	,SUM(E.bounced) AS Email_bounced
	,SUM(E.delivered) AS Email_delivered
	
	INTO #Email_month
	FROM [CLIENT_ANALYTICS].[dbo].[RPT_email_daily] E (NOLOCK)
	WHERE E.send_date  >=@StartDate
	AND E.clientid <> 'Warmup Tes'
	GROUP BY 
	 E.crm
	,DATEADD(m, DATEDIFF(m, 0, E.send_date), 0) 
	,E.clientid    

CREATE INDEX IX_Month_Date_Email
ON #Email_month (MONTH_DATE)

CREATE INDEX IX_CRM_Email
ON #Email_month (crm)

CREATE INDEX IX_ClientId_Email
ON #Email_month (clientid)
---------------------------------------------------#Letters_month--------------------------------------------------------------
DROP TABLE IF EXISTS #Letters_month;

	SELECT
      dss.SourceSystem
    , dd.MonthDate
    , dcl.ClientId
    , COUNT(fcl.KeyCustomerLetter) AS letters_sent
	INTO #Letters_month
	FROM DW_MSTR_DM.dbo.FactCustomerLetter fcl WITH (NOLOCK)

	JOIN DW_MSTR_DM.dbo.DimDate dd WITH (NOLOCK)
    ON fcl.KeyDate_MailMonth = dd.KeyDate 

	JOIN DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK)
    ON fcl.KeySourceSystem = dss.KeySourceSystem

	JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK)
    ON fcl.KeyClient = dcl.KeyClient   

	WHERE dd.MonthDate >= @StartDate
	AND CASE WHEN (dd.MonthDate >= '2025-08-01' AND fcl.TypeFlag IN ('P','B'))                                       -- As per Debbie, P = Print , B = Both Print and Email --Data available from August 2025
			    OR fcl.KeySourceSystem IN (3,7)                                                                      -- Type flag is not available for Amex and FAST
			OR dd.MonthDate < '2025-08-01' 
			 THEN 1 
			 ELSE 0 END = 1

	GROUP BY
      dss.SourceSystem
    , dd.MonthDate
    , dcl.ClientId 

	UNION ALL

	SELECT 

	 SourceSystem = 'FACS'
	,DATEADD(m, DATEDIFF(m, 0, lf.MAIL_DATE), 0) AS MONTH_DATE
	,lf.CLIENT_ID
	,COUNT(lf.LETTER_FACT_ID) AS letters_sent

	FROM DW_MSTR_DM.dbo.LETTER_FACT lf WITH (NOLOCK) 
	WHERE lf.MAIL_DATE  >= @StartDate
	GROUP BY 
	 DATEADD(m, DATEDIFF(m, 0, lf.MAIL_DATE), 0)
	,lf.CLIENT_ID




CREATE INDEX IX_Month_Date_Letters
ON #Letters_month (MonthDate)

CREATE INDEX IX_SourceSystem_Letters
ON #Letters_month (SourceSystem)

CREATE INDEX IX_ClientId_Letters
ON #Letters_month (clientid)
-------------------------------------------------------join all the temp tables to get master data-------------------------------
	  
DROP TABLE IF EXISTS  #t
 SELECT
       clnt.MONTH_DATE
	  ,clnt.SourceSystem AS CRM
	  ,clnt.ClientStream
	  ,clnt.ClientStreamId
	  ,clnt.ClientParent
      ,clnt.ClientId
      --,clnt.CALNDR_DT
      --,clnt.Week_ID
	  --------------------------------------------------------------------------------------------------------------
	  ,placed.LISTMONTH	  
      --,placed.LISTDATE
	  --------,placed.SourceSystem AS Placed_CRM
	  --------,placed.CLIENTID AS Placed_ClientID
      ,placed.Placed_Ct 
      ,placed.Placed_Amt
	  ---------------------------------------------------------------------------------------------------------------
      --------,calls.[MONTH_DATE] AS calls_MONTH_DATE
	  --------,calls.[crm]
	  ,calls.[KeySourceSystem]
      --------,calls.[CLIENT_ID]
      ,calls.[calls]
	  ,calls.[Calls_Outbound]
	  ,calls.[Calls_Inbound]
	  ,calls.[Calls_Manual]
	  ,calls.[Calls_Dialer]
	  ,calls.[rpcs]
	  ,calls.[RPCs_Inbound]
	  ,calls.[RPCs_Manual]
	  ,calls.[RPCs_Dialer]
      ,calls.[connects]
	  ,calls.[Connects_Inbound]
	  ,calls.[Connects_Manual]
	  ,calls.[Connects_Dialer]
      ,calls.[promises]
	  ,calls.[Promises_Inbound]
	  ,calls.[Promises_Manual]
	  ,calls.[Promises_Dialer]
	  ,calls.[Call_Duration_Seconds]
      ,calls.[talk_time]
      ,calls.[AGENT_UPDATE_TIME]
	  -------------------------------------------------------------------------------------------------------------------
	  --------,Accts.[MONTH_DATE] AS Acct_MONTH_DATE
	  --------,Accts.[crm] AS Acct_CRM
	  --------,Accts.[KeySourceSystem] AS Acct_KeySourceSystem
      --------,Accts.[CLIENT_ID] AS Acct_Client_ID
	  ,Accts.Accts_Worked
	  ---------------------------------------------------------------------------------------------------------------- 
	  --------,pymt.[MONTH_DATE] AS pymt_MONTH_DATE
	  --------,pymt.[crm] AS pymt_crm
      --------,pymt.[CLIENT_ID] AS pymt_CLIENT_ID
	  --------,pymt.[Accts_worked] AS pymt_Accts_Worked
      ,pymt.[total_collections]
      ,pymt.[total_fees]
	  ,pymt.[total_payments]
	  ,pymt.[first_payment_collections]
	  ,pymt.[first_payment_collections_count]
	  -------------------------------------------------------------------------------------------------
	  --------,inv.[rpt_month]
      --------,inv.[KeySourceSystem] AS inv_KeySourceSystem
      --------,inv.[ClientId] AS inv_ClientID
      ,inv.[accounts]

	  ----------------------------------------------------------------------------------------------------------
	  ,email.[Email_sent]
	  -------------,email.[Email_delivered]
	  -------------,email.[Email_bounced]

	  -----------------------------------------------------------------------------------------------------
	  ,Letters.[letters_sent]
	  INTO #t
	  FROM #ClientKPIMonths clnt
    LEFT JOIN #calls_month calls
    ON  clnt.MONTH_DATE = calls.MONTH_DATE
	AND clnt.SourceSystem = calls.crm
    AND clnt.ClientId = calls.CLIENT_ID

	LEFT JOIN CLIENT_ANALYTICS.dbo.RPT_Calls_Acct_Worked_Monthly (NOLOCK) Accts   -------Need to execute [CLIENT_ANALYTICS].[dbo].[usp_RPT_Calls_Acct_Worked_Monthly_ins_curr_month] and [CLIENT_ANALYTICS].[dbo].[usp_RPT_Calls_Acct_Worked_Monthly_ins_prev_month];
	ON clnt.MONTH_DATE = Accts.MONTH_DATE
	AND clnt.SourceSystem = Accts.crm
    AND clnt.ClientId = Accts.CLIENT_ID

	LEFT JOIN #pymt_month pymt
	ON  CAST(clnt.MONTH_DATE  AS DATE) = pymt.MONTH_DATE
	AND clnt.SourceSystem = pymt.crm
	AND clnt.ClientId = pymt.CLIENT_ID

	LEFT JOIN #ClientKPIPlacements placed
    ON  CAST(clnt.MONTH_DATE  AS DATE) = placed.LISTMONTH
	AND clnt.SourceSystem = placed.SourceSystem
    AND clnt.ClientID = placed.CLIENTID 

	LEFT JOIN #inv inv
	ON  CAST(calls.month_date AS DATE) = inv.rpt_month
	AND calls.KeySourceSystem = inv.KeySourceSystem
	AND calls.CLIENT_ID = inv.ClientId

	LEFT JOIN #Email_month email                                                                 ---------------------------------Email
	ON  clnt.MONTH_DATE = email.MONTH_DATE
	AND clnt.SourceSystem = email.crm
	AND clnt.ClientId = email.clientid

	LEFT JOIN #Letters_month Letters                                                             ---------------------------------Letters
	ON  clnt.MONTH_DATE = Letters.MonthDate
	AND clnt.SourceSystem = Letters.SourceSystem
	AND clnt.ClientId = Letters.ClientId


-------------------------------------------------------Load the master data into RPT table--------------------------------------------
TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Client_KPI_ScoreCard
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Client_KPI_ScoreCard
(
MONTH_DATE
      ,CRM
      ,ClientStream
      ,ClientStreamId
      ,ClientParent
      ,ClientId
      ,LISTMONTH
      ,Placed_Ct
      ,Placed_Amt
      ,KeySourceSystem
      ,calls
      ,Calls_Outbound
      ,Calls_Inbound
      ,Calls_Manual
      ,Calls_Dialer
      ,rpcs
      ,RPCs_Inbound
      ,RPCs_Manual
      ,RPCs_Dialer
      ,connects
      ,Connects_Inbound
      ,Connects_Manual
      ,Connects_Dialer
      ,promises
      ,Promises_Inbound
      ,Promises_Manual
      ,Promises_Dialer
      ,Call_Duration_Seconds
      ,talk_time
      ,AGENT_UPDATE_TIME
      ,Accts_Worked
      ,total_collections
      ,total_fees
      ,total_payments
      ,first_payment_collections
      ,first_payment_collections_count
      ,accounts
	  ,Email_sent
	  --,Email_delivered
	  --,Email_bounced
	  ,letters_sent
)
SELECT 
--* 
       [MONTH_DATE]
      ,[CRM]
      ,[ClientStream]
      ,[ClientStreamId]
      ,[ClientParent]
      ,[ClientId]
      ,[LISTMONTH]
      ,[Placed_Ct]
      ,[Placed_Amt]
      ,[KeySourceSystem]
      ,[calls]
      ,[Calls_Outbound]
      ,[Calls_Inbound]
      ,[Calls_Manual]
      ,[Calls_Dialer]
      ,[rpcs]
      ,[RPCs_Inbound]
      ,[RPCs_Manual]
      ,[RPCs_Dialer]
      ,[connects]
      ,[Connects_Inbound]
      ,[Connects_Manual]
      ,[Connects_Dialer]
      ,[promises]
      ,[Promises_Inbound]
      ,[Promises_Manual]
      ,[Promises_Dialer]
      ,[Call_Duration_Seconds]
      ,[talk_time]
      ,[AGENT_UPDATE_TIME]
      ,[Accts_Worked]
      ,[total_collections]
      ,[total_fees]
      ,[total_payments]
      ,[first_payment_collections]
      ,[first_payment_collections_count]
      ,[accounts]
	  ,[Email_sent]
	 -- ,[Email_delivered]
	--  ,[Email_bounced]
	  ,[letters_sent]
FROM #t


---SELECT * FROM CLIENT_ANALYTICS.dbo.RPT_Client_KPI_ScoreCard

END;