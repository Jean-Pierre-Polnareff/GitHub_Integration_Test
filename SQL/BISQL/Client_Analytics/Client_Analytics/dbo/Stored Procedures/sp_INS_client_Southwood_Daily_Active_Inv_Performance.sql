USE [CLIENT_ANALYTICS]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






-- ====================================================================
--  Populate daily active inventory performance for Southwood client 
-- ====================================================================
CREATE PROCEDURE [dbo].[sp_INS_client_Southwood_Daily_Active_Inv_Performance]
@StartDate			datetime = NULL, 
@EndDate			datetime = NULL 

AS 


BEGIN

SET NOCOUNT ON; 

SELECT @StartDate = ISNULL(@StartDate, CAST(DATEADD(DAY,-1,GETDATE()) AS date)), 
		@EndDate = ISNULL(@EndDate, CAST(DATEADD(DAY,0,GETDATE()) AS date)) 

	DELETE FROM [CLIENT_ANALYTICS].[dbo].[RPT_client_Southwood_Daily]
	WHERE rpt_date = CAST(@StartDate as date)   
	      
	--#act_prev
	IF OBJECT_ID('tempdb..#act_prev') IS NOT NULL
	DROP TABLE #act_prev;
	SELECT   dcu.keysourcesystem
		   , dcu.KeyCustomer
		   , dcl.ClientParent
		   , dcl.ClientId
		   , dcu.InitialBalance
		   , CASE WHEN datediff(month,dcu.ListDate,GETDATE()) < 7 THEN 'A - 0-6 months'
				  WHEN datediff(month,dcu.ListDate,GETDATE()) between 7 AND 12 THEN 'B - 7-12 months'
				  WHEN datediff(month,dcu.ListDate,GETDATE()) between 13 AND 24 THEN 'C - 1-2 years'
				  WHEN datediff(month,dcu.ListDate,GETDATE()) between 25 AND 36 THEN 'D - 2-3 years'
				  WHEN datediff(month,dcu.ListDate,GETDATE()) >36 THEN 'E - 3+ years'
				  END AS list_age
		   , CASE WHEN dcu.InitialBalance<50 THEN 'A - Less than $50'
				  ELSE 'B - $50+'
				  END AS balance_group
		   , @StartDate AS rpt_date    
		   , dcu.CustomerState
		   , dcu.CustomerId
		   , dcu.StatusCode
		   , dcu.SourceSystem
	INTO #act_prev
	FROM DW_MSTR_DM.dbo.DimCustomer dcu WITH (NOLOCK)
		JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) ON dcu.ClientId = dcl.ClientId
		                                          AND dcu.sourcesystem = dcl.sourcesystem
	WHERE dcu.ListDate <= @EndDate                
		  AND dcu.ListDate != '12/31/9999'
		  AND (dcu.CancelDate >= @StartDate OR dcu.CancelDate IS NULL)
		  AND dcl.ClientId IN ('STHWOD', 'STHWD2', 'STHWD3', 'STHWD4', 'STHWOS')   --Southwood clientIds list
		  AND dcu.keysourcesystem = 2

	--All Calls & SMS
	IF OBJECT_ID('tempdb..#calls_raw') IS NOT NULL
	DROP TABLE #calls_raw;		
	SELECT    a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId
			, a.list_age
			, a.balance_group
			, fcc.KeyCustomerCall
			, a.CustomerId
			, fcc.IsConnect  
			, fcc.IsRPC 
			, a.CustomerState
			, fcc.IsInbound 
			, fcc.IsOutbound 
			, fcc.IsSMS
	INTO #calls_raw 
	FROM #act_prev a
		JOIN DW_MSTR_DM.dbo.FactCustomerCall fcc WITH (NOLOCK) ON a.KeyCustomer = fcc.KeyCustomer
	WHERE fcc.KeyDate_CallDate BETWEEN CONVERT(VARCHAR, @StartDate, 112) AND CONVERT(VARCHAR, @EndDate, 112) 


	--All Calls & SMS
	IF OBJECT_ID('tempdb..#calls') IS NOT NULL
	DROP TABLE #calls;	
    SELECT   a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId
			, a.list_age
			, a.balance_group
			, a.CustomerState
			, COUNT(a.KeyCustomerCall) AS calls
			, COUNT(CASE WHEN a.IsConnect=1 THEN a.KeyCustomerCall END) AS connected_calls
			, COUNT(CASE WHEN a.IsRPC=1 THEN a.KeyCustomerCall END) AS rpc_calls
			, COUNT(CASE WHEN a.IsSMS=1 THEN a.KeyCustomerCall END) AS sms_calls
	INTO #calls
	FROM #calls_raw a 
	GROUP BY a.rpt_date
			, a.KeySourceSystem 
			, a.ClientParent
			, a.ClientId
			, a.list_age
			, a.balance_group
			, a.CustomerState 
	CREATE INDEX #combo2 ON #calls(rpt_date,KeySourceSystem,ClientId,balance_group,list_age,CustomerState);

	
	--letters
	IF OBJECT_ID('tempdb..#letters') IS NOT NULL
		DROP TABLE #letters;

	SELECT a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId
			, a.list_age
			, a.balance_group
			, COUNT(fcl.KeyCustomerLetter) AS letters
			, COUNT(DISTINCT a.CustomerId) AS unq_letters
			, a.CustomerState
	INTO #letters
	FROM #act_prev a
				JOIN DW_MSTR_DM.dbo.FactCustomerLetter fcl WITH (NOLOCK) ON a.KeyCustomer =   fcl.KeyCustomer
			WHERE fcl.KeyDate_MailDate BETWEEN CONVERT(VARCHAR, @StartDate, 112) AND CONVERT(VARCHAR, @EndDate, 112) 
	GROUP BY  a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId   
			, a.list_age
			, a.balance_group
			, a.CustomerState

	CREATE INDEX #combo3 ON #letters(rpt_date,KeySourceSystem,ClientId,balance_group,list_age,CustomerState);


	-- Emails
	IF OBJECT_ID('tempdb..#emails') IS NOT NULL
		DROP TABLE #emails;
	  SELECT  a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId
			, a.list_age
			, a.balance_group
			, COUNT(rem.KeyEmailReport) AS emails
			, COUNT(DISTINCT rem.[GUID]) AS unq_emails
			, a.CustomerState
	INTO #emails
	FROM #act_prev a
				JOIN DW_MSTR_DM.dbo.Radius_EmailReportData rem WITH (NOLOCK) ON a.KeyCustomer =  rem.KeyCustomer
			WHERE rem.EventDate BETWEEN @StartDate and  @EndDate 
				and rem.[EventValue]='EMAIL_SENT'
	GROUP BY  a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId   
			, a.list_age
			, a.balance_group
			, a.CustomerState

	CREATE INDEX #combo4 ON #emails(rpt_date,KeySourceSystem,ClientId,balance_group,list_age,CustomerState);
	

	--payment
	IF OBJECT_ID('tempdb..#pay') IS NOT NULL
		DROP TABLE #pay;

	   SELECT a.rpt_date
			, a.KeySourceSystem
			, a.ClientParent
			, a.ClientId
			, a.list_age
			, a.balance_group
			, COUNT(fcp.KeyCustomerPayment) AS payers
			, COUNT(DISTINCT a.CustomerId) AS unq_payers
			, SUM(fcp.PaymentAppliedAmt) AS payments
			, a.CustomerState
	INTO #pay
	FROM #act_prev a
		JOIN DW_MSTR_DM.dbo.FactCustomerPayment fcp WITH (NOLOCK) ON a.KeyCustomer = fcp.KeyCustomer
		JOIN DW_MSTR_DM.dbo.DimPaymentType dpt WITH (NOLOCK)  ON fcp.KeyPaymentType = dpt.KeyPaymentType
										   AND (dpt.PaymentCategory != 'Adjustment' OR dpt.PaymentCategory IS NULL)
										   AND dpt.PaymentType NOT IN ('DA','DAR')
	WHERE fcp.KeyDate_PaymentDate BETWEEN CONVERT(VARCHAR, @StartDate, 112) AND CONVERT(VARCHAR, @EndDate, 112)  
	GROUP BY a.rpt_date
		   , a.KeySourceSystem
		   , a.ClientParent
		   , a.ClientId
		   , a.list_age
		   , a.balance_group
		   , a.CustomerState;

	CREATE INDEX #combo4 ON #pay(rpt_date,KeySourceSystem,ClientId,balance_group,list_age,CustomerState);


	IF OBJECT_ID('tempdb..#accts') IS NOT NULL
	DROP TABLE #accts;
	      SELECT rpt_date
			   , KeySourceSystem
			   , ClientParent
			   , ClientId
			   , list_age
			   , balance_group
			   , COUNT(distinct CustomerId) AS accounts
			   , SUM(InitialBalance) AS balances
			   , SUM(InitialBalance) / COUNT(distinct CustomerId) AS avg_balance
			   , CustomerState
	INTO #accts
	FROM #act_prev
	GROUP BY  rpt_date
			, KeySourceSystem
			, ClientParent
			, ClientId
			, list_age
			, balance_group
			, CustomerState;


	IF OBJECT_ID('tempdb..#clientid_agg') IS NOT NULL
	DROP TABLE #clientid_agg;			 
	SELECT ac.rpt_date
			, ac.KeySourceSystem
			, ac.ClientParent
			, ac.ClientId
			, ac.list_age
			, ac.balance_group
			, ac.accounts
			, ac.balances
			, ac.CustomerState
			, c.calls
			, c.rpc_calls
			, c.connected_calls
			, l.letters
			, l.unq_letters
			, c.sms_calls
			, e.emails
			, e.unq_emails
            , p.payers
			, p.unq_payers
			, p.payments
	INTO #clientid_agg
	FROM #accts ac
		LEFT JOIN #calls c ON ac.rpt_date=c.rpt_date
					AND ac.KeySourceSystem=c.KeySourceSystem
					AND ac.ClientId=c.ClientId
					AND ac.balance_group=c.balance_group
					AND ac.list_age=c.list_age
					AND ac.CustomerState=c.CustomerState
		LEFT JOIN #letters l ON ac.rpt_date=l.rpt_date
					AND ac.KeySourceSystem=l.KeySourceSystem
					AND ac.ClientId=l.ClientId
					AND ac.balance_group=l.balance_group
					AND ac.list_age=l.list_age
					AND ac.CustomerState=l.CustomerState
		LEFT JOIN #emails e ON ac.rpt_date=e.rpt_date
					AND ac.KeySourceSystem=e.KeySourceSystem
					AND ac.ClientId=e.ClientId
					AND ac.balance_group=e.balance_group
					AND ac.list_age=e.list_age
					AND ac.CustomerState=e.CustomerState
		LEFT JOIN #pay p ON ac.rpt_date=p.rpt_date
					AND ac.KeySourceSystem=p.KeySourceSystem
					AND ac.ClientId=p.ClientId
					AND ac.balance_group=p.balance_group
					AND ac.list_age=p.list_age
					AND ac.CustomerState=p.CustomerState

	    INSERT INTO  [CLIENT_ANALYTICS].[dbo].[RPT_client_Southwood_Daily]
		            (
					  rpt_date
					, KeySourceSystem
					, ClientParent
					, ClientId
					, list_age
					, balance_group
					, CustomerState
					, accounts
					, balances
					, calls
					, rpc_calls
					, connected_calls
					, letters
					, unq_letters
					, sms_calls
					, emails
					, unq_emails
                    , payments
					, payers
					, unq_payers
					)
	SELECT rpt_date
			, KeySourceSystem
			, clientparent
			, ClientId
			, list_age
			, balance_group
			, CustomerState
			, SUM(accounts) AS accounts
			, SUM(balances) AS balances
			, SUM(calls) AS calls
			, SUM(rpc_calls) AS rpc_calls
		    , SUM(connected_calls) as connected_calls
			, SUM(letters) AS letters
			, SUM(unq_letters) AS unq_letters
			, SUM(sms_calls) AS sms_calls
			, SUM(emails) as emails
			, SUM(unq_emails) as unq_emails
			, SUM(payments) AS payments
			, SUM(payers) AS payers
			, SUM(unq_payers) AS unq_payers
		FROM #clientid_agg
		GROUP BY rpt_date
			   , KeySourceSystem
			   , ClientParent
			   , ClientId
			   , list_age
			   , balance_group
			   , CustomerState;
END;

GO


