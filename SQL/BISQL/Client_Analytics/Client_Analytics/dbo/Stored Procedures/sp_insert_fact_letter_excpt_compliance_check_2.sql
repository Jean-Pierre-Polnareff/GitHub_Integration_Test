--exec  [dbo].[sp_insert_fact_letter_excpt_compliance_check] @startdate = '2023-01-01', @end =  '2023-08-23'
--go 

CREATE PROCEDURE [dbo].[sp_insert_fact_letter_excpt_compliance_check]              
@startdate datetime = NULL,
@end datetime =  Null
       
AS   
    
BEGIN
	SET NOCOUNT ON;

	SELECT @startdate = DATEADD(dd,-8, ISNULL(@startdate,DATEADD(day, 1, MAX(CONTACT_DATE))))   
	FROM CLIENT_ANALYTICS.[dbo].[fact_letter_excpt_CRM_level_count] WITH (NOLOCK) 
	WHERE ltr_expt_id = 1 
	
	SELECT @end = ISNULL(@end, DATEADD(day, -1, GETDATE())); 
 
	/*
	DROP TABLE IF EXISTS #temp_fcc 

	SELECT fcc.KeyCustomerCall, fcc.KeyCustomer, fcc.KeyDate_CallDate  
	INTO #temp_fcc 
	FROM DW_MSTR_DM.dbo.FactCustomerCall fcc WITH (NOLOCK)  
	WHERE KeyDate_CallDate BETWEEN CONVERT(VARCHAR,CAST(@startdate AS DATE),112) AND CONVERT(VARCHAR,CAST(@end AS DATE),112) 

	DROP TABLE IF EXISTS #temp_customer  

	SELECT DISTINCT c.KeyCustomer           
	INTO #temp_customer 
	FROM #temp_fcc t 
		JOIN DW_MSTR_DM.dbo.DimCustomer c WITH (NOLOCK) ON c.KeyCustomer = t.KeyCustomer 
	WHERE c.StatusCode != 'DW_deactivate' 
		AND c.ClientId NOT LIKE 'DC%P' 
		AND c.ClientId NOT IN ('SNBCEP', 'EMPBSP','EMPCMF','EMPIFP') 

	DROP TABLE IF EXISTS #temp_customer_firstevercontact 

	SELECT fcc.KeyCustomer, min(KeyDate_CallDate) KeyDate_CallDate      
	INTO #temp_customer_firstevercontact 
	FROM #temp_customer t   
		JOIN DW_MSTR_DM.dbo.FactCustomerCall fcc WITH (NOLOCK) ON fcc.KeyCustomer = t.KeyCustomer 
		JOIN DW_MSTR_DM.dbo.DimDialerResult r with (nolock) on r.KeyDialerResult = fcc.KeyDialerResult 
	WHERE fcc.KeyDate_CallDate < CONVERT(VARCHAR,CAST(@startdate AS DATE),112)  
		and (fcc.IsRPC = 1 
				or r.Dialer_Result like '%Left Message%') 
	GROUP BY fcc.KeyCustomer
	*/ 

	-- COLLECT AMEX first party clients 
	DROP TABLE IF EXISTS #AmexClient_FirstParty_Codes 

	SELECT cl.KeyClient, cl.ClientId, cl.SourceSystem  
	INTO #AmexClient_FirstParty_Codes 
	FROM [HVDB02.CORPGLBDOM.LOCAL].Amex.dbo.Amex_ClientCodes_LookupTable c WITH (NOLOCK) 
		JOIN dw_mstr_dm.dbo.DimClient cl ON cl.clientid = c.clientcode 
	WHERE c.firstpartyflag = 1 
		AND cl.SourceSystem = 'Amex Latitude' 

	-------------DELETE the existing records from CLIENT_ANALYTICS.dbo.[fact_letter_excpt_CRM_level_count] for Yesterday------------------------------------------------------------------


	DELETE  
	FROM  CLIENT_ANALYTICS.dbo.[fact_letter_excpt_CRM_level_count]
	WHERE contact_date BETWEEN ISNULL(@startdate, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) 
						AND ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	AND ltr_expt_id in (1,3,4) 
	AND KeySourceSystem not in (0,4)  
  
	DELETE 
	FROM CLIENT_ANALYTICS.dbo.fact_Letter_excpt 
	WHERE contact_date BETWEEN ISNULL(@startdate, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) 
						AND ISNULL(@end, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0))
	AND ltr_expt_id in (1,3,4) 
	AND KeySourceSystem not in (0,4) 

	DROP TABLE IF EXISTS #temp_cust 
	DROP TABLE IF EXISTS #temp_cust_first_call_connect 
	DROP TABLE IF EXISTS #temp_cust_first_call_connect_dlr
	DROP TABLE IF EXISTS #temp_cust_IDN_letters 
 
	SELECT c.*, cl.ClientParent         
	INTO #temp_cust 
	FROM DW_STAGING.dbo.FCC_FirstEverContact_Lookup t WITH (NOLOCK) 
		JOIN dw_mstr_dm.dbo.DimCustomer c with (nolock) on c.keycustomer = t.keycustomer 
		JOIN DW_MSTR_DM.dbo.DimClient cl WITH (NOLOCK) ON cl.KeyClient = c.KeyClient 
		LEFT JOIN #AmexClient_FirstParty_Codes fp  ON fp.KeyClient = cl.KeyClient 
	WHERE c.StatusCode != 'DW_deactivate' 
		AND fp.ClientId IS NULL 
		AND cl.ClientId NOT IN ('VTRAE1' ,'VSTRAFIRST'                   --11/13/25 As per Greg S, it is a first party client on Thirdpord and we don’t send letters.. Hence, removing from the report
		,'VTR4C1','VTRAE1', 'VTREE1', 'VTRVE1'                           --12/04/25 As per Greg S, it is a first party client on Thirdpord and we don’t send letters.. Hence, removing from the report
		)
	SELECT t.KeyCustomer, t.KeyClient, MIN(callstarttime) FirstContactCall       
	INTO #temp_cust_first_call_connect  
	FROM DW_MSTR_DM.dbo.FactCustomerCall fcc with (nolock)   
		JOIN #temp_cust t ON t.KeyCustomer = fcc.KeyCustomer 	
							AND t.KeyClient = fcc.KeyClient
	WHERE fcc.KeyDate_CallDate >= CONVERT(VARCHAR,CAST(ISNULL(@startdate, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS DATE),112)  
		AND (IsConnect = 1 or IsRpc = 1)  
		AND t.ClientId NOT LIKE 'DC%P' 
		AND t.ClientId NOT IN ('SNBCEP', 'EMPBSP','EMPCMF','EMPIFP')  
		AND t.StatusCode != 'DW_deactivate' 
	GROUP BY t.KeyCustomer, t.KeyClient 
 
	SELECT DISTINCT T.*, MIN(fcc.KeyCustomerCall) KeyCustomerCall, 
	CASE WHEN fcc.IsOutbound = 1 
		THEN 'Outbound' 
		ELSE 
			CASE WHEN fcc.IsInbound = 1 THEN 'Inbound' ELSE NULL END 
		END Direction 
	, r.Dialer_Result, e.Result_Code, e.LiveVox_Result, e.Result_Description, e.Comments, e.Process, e.IsActive 
	INTO #temp_cust_first_call_connect_dlr 
	FROM #temp_cust_first_call_connect t 
		JOIN DW_MSTR_DM.dbo.FactCustomerCall fcc WITH (NOLOCK) ON fcc.KeyCustomer = t.KeyCustomer 
											AND fcc.KeyClient = t.KeyClient 
											AND fcc.CallStartTime = t.FirstContactCall  
		JOIN DW_MSTR_DM.dbo.DimDialerResult r WITH (NOLOCK) ON r.KeyDialerResult = fcc.KeyDialerResult  
		JOIN DW_MSTR_DM.dbo.LetterResultCodeExceptions e WITH (NOLOCK) ON e.livevox_result = r.dialer_result
	WHERE e.IsActive = 1    
		AND fcc.KeyDate_CallDate >= CONVERT(VARCHAR,CAST(ISNULL(@startdate, DATEADD(day,DATEDIFF(day, 0, DATEADD(day, -1, GETDATE()) ) ,0)) AS DATE),112)   
	GROUP BY t.KeyCustomer, t.KeyClient, t.FirstContactCall, 
		CASE WHEN fcc.IsOutbound = 1 
		THEN 'Outbound' 
		ELSE 
			CASE WHEN fcc.IsInbound = 1 THEN 'Inbound' ELSE NULL END 
		END,  
		r.Dialer_Result, e.Result_Code, e.LiveVox_Result, e.Result_Description, e.Comments, e.Process, e.IsActive    
	
	DROP TABLE IF EXISTS #temp_cust_IDN_letters 

	SELECT x.KeyCustomer, x.KeyClient,  x.KeyDate_MailDate  , lt.LetterType
	INTO #temp_cust_IDN_letters
	FROM (SELECT fcl.KeyCustomer, fcl.KeyClient, MIN(KeyDate_MailDate) KeyDate_MailDate    
	
	FROM DW_MSTR_DM.dbo.FactCustomerLetter fcl WITH (NOLOCK) 
		JOIN #temp_cust t ON t.KeyCustomer = fcl.KeyCustomer 
							AND t.KeyClient = fcl.KeyClient 
		JOIN DW_MSTR_DM.dbo.DimLetterType lt ON lt.KeyLetterType = fcl.KeyLetterType 
	WHERE lt.LetterType like 'IDN%'   
	GROUP BY fcl.KeyCustomer, fcl.KeyClient
	)x
	 JOIN DW_MSTR_DM.dbo.FactCustomerLetter fcl WITH (NOLOCK) 
		ON x.KeyCustomer = fcl.KeyCustomer 
			AND x.KeyClient = fcl.KeyClient 
			AND x.KeyDate_MailDate = fcl.KeyDate_MailDate
		LEFT JOIN DW_MSTR_DM.dbo.DimLetterType lt 
		ON lt.KeyLetterType = fcl.KeyLetterType 

	-- REMOVE IF MAILED BEFORE 
	DELETE c 
	FROM #temp_cust c 
		JOIN #temp_cust_first_call_connect_dlr cll ON c.KeyCustomer = cll.KeyCustomer 
												AND c.KeyClient = cll.KeyClient 									
		LEFT JOIN #temp_cust_IDN_letters ltr ON ltr.KeyCustomer = cll.KeyCustomer 
												AND ltr.KeyClient = cll.KeyClient 
	WHERE ltr.KeyDate_MailDate <= CONVERT(VARCHAR,CAST(cll.FirstContactCall AS DATE),112) 
		OR CONVERT(VARCHAR,CAST(c.IDL_Date AS DATE),112)  <= CONVERT(VARCHAR,CAST(cll.FirstContactCall AS DATE),112) 

	--------exceptions- No IDL letter sent within 5 days of call made to customer-------------

	DROP TABLE IF EXISTS #exceptions 

	SELECT ltr_expt_id = 3,
		c.CustomerId, 
		c.ConsumerId, 
		c.StatusCode, 
		c.ListDate, 
		c.ClientId, 
		c.ClientParent Client_Name,  
		CAST(cll.FirstContactCall AS DATE) Contact_Date,
		cll.Dialer_Result, 
		cll.LiveVox_Result,  
		CONCAT(c.FirstName, ' ' , c.LastName) Name_on_Account, 
		c.CurrentBalance Balance, 
		Is_PIF = CASE WHEN ISNULL(c.InitialBalance,0) = ISNULL(c.PaidOnAccountAmt,0) THEN 1 ELSE 0 END, 
		Is_SIF = CASE WHEN ISNULL(c.SIFAmt, 0) > 0					
					  THEN 1 ELSE 0 END,
		c.LastPaymentDate, 
		isnull(c.idl_date, c.ReturnMailDate) Letter_Sent_Date,
		c.ReturnMailDate,
		c.SourceSystem,
		c.KeySourceSystem, 
		cll.Direction, 
		ltr.LetterType      
	INTO #exceptions   
	FROM #temp_cust c 
		JOIN #temp_cust_first_call_connect_dlr cll ON cll.KeyCustomer = c.KeyCustomer 
												AND cll.KeyClient = c.KeyClient 
		LEFT JOIN #temp_cust_IDN_letters ltr ON ltr.KeyCustomer = c.KeyCustomer  
												AND ltr.KeyClient = c.KeyClient 
	WHERE cll.FirstContactCall IS NOT NULL 
		AND DATEDIFF(DD,CAST(cll.FirstContactCall AS DATE),ISNULL(CAST(CAST(ltr.KeyDate_MailDate AS VARCHAR) AS DATE),CAST(GETDATE() AS DATE))) > 3 
		AND DATEDIFF(DD,CAST(cll.FirstContactCall AS DATE),ISNULL(CAST(c.IDL_Date AS DATE),CAST(GETDATE() AS DATE))) > 3   
		AND c.CustomerId not in ('11644','1224851','1687','1028999','14892599')
	ORDER BY c.KeyCustomer

	INSERT INTO #exceptions 
	(
		ltr_expt_id,
		CustomerId,
		ConsumerId,
		StatusCode,
		ListDate,
		ClientId,
		Client_Name,
		Contact_Date,
		Dialer_Result,
		LiveVox_Result,
		Name_on_Account,
		Balance,
		Is_PIF,
		Is_SIF,
		LastPaymentDate,
		Letter_Sent_Date,
		ReturnMailDate,
		SourceSystem,
		KeySourceSystem,
		Direction,
		LetterType
	)
	SELECT ltr_expt_id = 1,
		c.CustomerId, 
		c.ConsumerId, 
		c.StatusCode, 
		c.ListDate, 
		c.ClientId, 
		c.ClientParent Client_Name,  
		CAST(cll.FirstContactCall AS DATE) Contact_Date,
		cll.Dialer_Result, 
		cll.LiveVox_Result,  
		CONCAT(c.FirstName, ' ' , c.LastName) Name_on_Account, 
		c.CurrentBalance Balance, 
		Is_PIF = CASE WHEN ISNULL(c.InitialBalance,0) = ISNULL(c.PaidOnAccountAmt,0) THEN 1 ELSE 0 END, 
		Is_SIF = CASE WHEN ISNULL(c.SIFAmt, 0) > 0					
					  THEN 1 ELSE 0 END,
		c.LastPaymentDate, 
		isnull(c.idl_date, c.ReturnMailDate) Letter_Sent_Date,
		c.ReturnMailDate,
		c.SourceSystem,
		c.KeySourceSystem, 
		cll.Direction,
		ltr.LetterType     
	FROM #temp_cust c 
		JOIN #temp_cust_first_call_connect_dlr cll ON cll.KeyCustomer = c.KeyCustomer 
												AND cll.KeyClient = c.KeyClient 
		LEFT JOIN #temp_cust_IDN_letters ltr ON ltr.KeyCustomer = c.KeyCustomer  
												AND ltr.KeyClient = c.KeyClient 
	WHERE cll.FirstContactCall IS NOT NULL 
		AND DATEDIFF(DD,CAST(cll.FirstContactCall AS DATE),ISNULL(CAST(CAST(ltr.KeyDate_MailDate AS VARCHAR) AS DATE),CAST(GETDATE() AS DATE))) > 5 
		AND DATEDIFF(DD,CAST(cll.FirstContactCall AS DATE),ISNULL(CAST(c.IDL_Date AS DATE),CAST(GETDATE() AS DATE))) > 5   
		AND c.CustomerId not in ('11644','1224851','1687','1028999','14892599')
	ORDER BY c.KeyCustomer
	

	INSERT INTO #exceptions 
	(
		ltr_expt_id,
		CustomerId,
		ConsumerId,
		StatusCode,
		ListDate,
		ClientId,
		Client_Name,
		Contact_Date,
		Dialer_Result,
		LiveVox_Result,
		Name_on_Account,
		Balance,
		Is_PIF,
		Is_SIF,
		LastPaymentDate,
		Letter_Sent_Date,
		ReturnMailDate,
		SourceSystem,
		KeySourceSystem,
		Direction,
		LetterType
	)
	SELECT ltr_expt_id = 4,
		c.CustomerId, 
		c.ConsumerId, 
		c.StatusCode, 
		c.ListDate, 
		c.ClientId, 
		c.ClientParent Client_Name,  
		CAST(cll.FirstContactCall AS DATE) Contact_Date,
		cll.Dialer_Result, 
		cll.LiveVox_Result,  
		CONCAT(c.FirstName, ' ' , c.LastName) Name_on_Account, 
		c.CurrentBalance Balance, 
		Is_PIF = CASE WHEN ISNULL(c.InitialBalance,0) = ISNULL(c.PaidOnAccountAmt,0) THEN 1 ELSE 0 END, 
		Is_SIF = CASE WHEN ISNULL(c.SIFAmt, 0) > 0					
					  THEN 1 ELSE 0 END,
		c.LastPaymentDate, 
		isnull(c.idl_date, c.ReturnMailDate) Letter_Sent_Date,
		c.ReturnMailDate,
		c.SourceSystem,
		c.KeySourceSystem, 
		cll.Direction,
		ltr.LetterType     
	FROM #temp_cust c 
		JOIN #temp_cust_first_call_connect_dlr cll ON cll.KeyCustomer = c.KeyCustomer 
												AND cll.KeyClient = c.KeyClient 
		LEFT JOIN #temp_cust_IDN_letters ltr ON ltr.KeyCustomer = c.KeyCustomer  
												AND ltr.KeyClient = c.KeyClient 
	WHERE cll.FirstContactCall IS NOT NULL 
		AND DATEDIFF(DD,CAST(cll.FirstContactCall AS DATE),ISNULL(CAST(CAST(ltr.KeyDate_MailDate AS VARCHAR) AS DATE),CAST(GETDATE() AS DATE))) > 8 
		AND DATEDIFF(DD,CAST(cll.FirstContactCall AS DATE),ISNULL(CAST(c.IDL_Date AS DATE),CAST(GETDATE() AS DATE))) > 8   
		AND c.CustomerId not in ('11644','1224851','1687','1028999','14892599')
	ORDER BY c.KeyCustomer


	-- LETTER_SENT_DATE CAN BE RETURNMAILDATE 
	DELETE 
	FROM #exceptions 
	WHERE ltr_expt_id = 1 
		AND (letter_sent_date < contact_date 
		OR DATEDIFF(dd, contact_date, letter_sent_date) < 5
		OR (Direction = 'Inbound' AND LiveVox_Result like '%Listened%')) 
 
	DELETE 
	FROM #exceptions 
	WHERE ltr_expt_id = 3  
		AND (letter_sent_date < contact_date 
		OR DATEDIFF(dd, contact_date, letter_sent_date) < 3
		OR (Direction = 'Inbound' AND LiveVox_Result like '%Listened%')) 

	DELETE 
	FROM #exceptions 
	WHERE ltr_expt_id = 4  
		AND (letter_sent_date < contact_date 
		OR DATEDIFF(dd, contact_date, letter_sent_date) < 8
		OR (Direction = 'Inbound' AND LiveVox_Result like '%Listened%')) 
   
   --Remove Operator Transfer (Caller Opted for Voice Mail) from the exceptions as per Greg 3/8/24
	DELETE 
	FROM #exceptions 
	WHERE ltr_expt_id IN (1,3,4)
		AND LiveVox_Result = 'Operator Transfer (Caller Opted for Voice Mail)'

 
	-----------Adding 0's into CLIENT_ANALYTICS.[dbo].[fact_letter_excpt_CRM_level_count] for all time----------------------------
	
	INSERT INTO CLIENT_ANALYTICS.[dbo].[fact_letter_excpt_CRM_level_count]

	(contact_date
	,keysourcesystem
	,ltr_expt_id
	,Count_of_Exceptions
	,Insert_Date)

	SELECT contact_date
	,keysourcesystem
	,ltr_expt_id
	,Count_of_Exceptions
	,Insert_Date

	FROM
	(
	SELECT e.contact_date 
		,e.keysourcesystem AS keysourcesystem 
		,e.ltr_expt_id AS ltr_expt_id
		,ISNULL(SUM(E.Count_of_Exceptions),0) AS Count_of_Exceptions
		,GETDATE() AS Insert_Date
		,c.No_of_Calls 
	FROM (SELECT CAST(exc.contact_date AS DATE) contact_date
				,exc.keysourcesystem
				,exc.ltr_expt_id
				,COUNT(*) AS Count_of_Exceptions
				FROM #exceptions exc 
				group by CAST(exc.contact_date AS DATE)
				,exc.keysourcesystem
				,exc.ltr_expt_id
		) E
		LEFT JOIN 
			( 
				SELECT 
				dt.CalendarDate AS calldate
				, fct.KeySourceSystem
				, COUNT(*) AS No_of_Calls 
				FROM  DW_MSTR_DM.dbo.FactCustomerCall fct (NOLOCK)
					JOIN DW_MSTR_DM.dbo.DimDate dt (NOLOCK) ON fct.KeyDate_CallDate = dt.KeyDate
				WHERE dt.CalendarDate BETWEEN  ISNULL(@startdate,getdate() - 1) AND ISNULL(@end,getdate() - 1) 
					AND fct.KeyDate_CallDate BETWEEN CONVERT(VARCHAR(50), CAST(ISNULL(@startdate,GETDATE() - 1) AS DATE), 112) AND CONVERT(VARCHAR(50), CAST(ISNULL(@end,GETDATE() - 1) AS DATE), 112)
				GROUP BY dt.CalendarDate  , fct.KeySourceSystem 
			)c ON e.contact_date = CAST(c.calldate AS DATE) 
				AND e.keysourcesystem = c.KeySourceSystem
		GROUP BY  
		e.contact_date  
		,e.keysourcesystem
		,e.ltr_expt_id   
		,c.No_of_Calls
		) f
	WHERE ISNULL(f.No_of_Calls,0) > 0

	------insert data into table CLIENT_ANALYTICS.dbo.fact_Letter_excpt at customer level-----

	INSERT INTO CLIENT_ANALYTICS.dbo.fact_Letter_excpt  
	(
				   ltr_expt_id 
				 , CUSTOMER_ID
				 , ConsumerId
				 , Status_Code
				 , List_Date
				 , CLIENT_ID
				 , Client_Name
				 , KeySourceSystem
				 , Sourcesystem
				 , Contact_Date 
				 , Name_on_Account 
				 , Balance 
				 , Is_SIF 
				 , Is_PIF 
				 , LastPaymentDate 
				 , Letter_Sent_Date 
				 , ReturnMailDate
				 , Dialer_Result 
				 , LiveVox_Result
				 , Direction 
				 , LetterType
				 , insert_date	
				 	 		
			   )
	
		SELECT   DISTINCT 
				   exc.ltr_expt_id 
				 , exc.customerid
				 , exc.ConsumerId
				 , exc.StatusCode
				 , exc.ListDate
				 , exc.ClientId
				 , exc.Client_Name
				 , exc.KeySourceSystem
				 , exc.SourceSystem
				 , exc.Contact_Date
				 , exc.Name_on_Account
				 , exc.Balance 
				 , exc.Is_SIF 
				 , exc.Is_PIF 
				 , exc.LastPaymentDate 
				 , exc.letter_sent_date 
				 , exc.ReturnMailDate
				 , exc.Dialer_Result 
				 , exc.LiveVox_Result 
				 , exc.Direction 
				 , exc.LetterType
				 , insert_date = getdate()
				 		   
	FROM #exceptions exc 

END;