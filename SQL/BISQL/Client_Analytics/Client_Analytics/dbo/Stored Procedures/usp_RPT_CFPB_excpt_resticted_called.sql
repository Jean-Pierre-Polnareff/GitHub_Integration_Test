
/*
	        
*/ 
 
CREATE PROCEDURE [dbo].[usp_RPT_CFPB_excpt_resticted_called] 

AS  
BEGIN 

	SET NOCOUNT ON; 


	/* 
		COLLECT EXCEPTIONS SINCE LAST REPORTED 
	*/
	DECLARE @max_excpt_date DATETIME; 
	SELECT @max_excpt_date = ISNULL(MAX([ExceptionDate]), CAST('2023-03-01' AS DATE)) FROM [CLIENT_ANALYTICS].[dbo].[RPT_CFPB_excpt_resticted_called] with (nolock) 
	 
	DROP TABLE IF EXISTS #dimcustomer_restricted

	/*                 
		COLLECT CUSTOMERS EVER RESTRICTED 
	*/
	SELECT KeyCustomer,
			KeyETLAuditHistory,
			CustomerId,
			ClientId,
			SourceSystem,
			ListDate, 
			ClientOpenDate,
			ClientScore,
			StatusCode,    
			CancelCode,
			OriginalCancelCode,
			CancelDate,
			ChargeOffDate,
			IsAccountWorked,
			InsertDate,
			UpdateDate,
			ConsumerId, 
			CustomerMatchId,
			KeySourceSystem,
			KeyClient,
			IDL_Date,
			LeadAccount,
			OfficeID,
			FirstName,  
			LastName, 
			CustomerState 
	INTO #dimcustomer_restricted 
	FROM DW_MSTR_DM.dbo.DimCustomer WITH (NOLOCK)     
	WHERE StatusCode IN (SELECT StatusCode FROM DW_MSTR_DM.dbo.DimRestriction WHERE [type] = 'CFPB')  

	DROP TABLE IF EXISTS #dimcustomer_previous_restriction
	
	/*
		GET EARLIEST RESTRICTION DATE 
	*/
	SELECT t.KeyCustomer,
			t.KeyETLAuditHistory,
			t.CustomerId,
			t.ClientId, 
			t.SourceSystem,
			t.ListDate,
			t.ClientOpenDate,
			t.ClientScore,  
			t.StatusCode,
			t.CancelCode,
			t.OriginalCancelCode,
			t.CancelDate,
			t.ChargeOffDate,
			t.IsAccountWorked,
			t.InsertDate,
			t.UpdateDate,
			t.ConsumerId,
			t.CustomerMatchId,
			t.KeySourceSystem,
			t.KeyClient,
			t.IDL_Date, 
			t.LeadAccount,
			t.OfficeID,
			t.FirstName, 
			t.LastName, 
			t.CustomerState,
			h.StatusCode StatusCodeHist, 
			min(HistoryId) HistoryId,
			min(h.InsertDate) InsertDateHist 
	INTO #dimcustomer_previous_restriction 
	FROM #dimcustomer_restricted t 
		JOIN DW_MSTR_DM.dbo.DimCustomerHistory h WITH (NOLOCK) ON h.KeyCustomer = t.KeyCustomer 
								AND h.StatusCode = t.StatusCode  
	GROUP BY t.KeyCustomer,
			t.KeyETLAuditHistory,
			t.CustomerId,
			t.ClientId,
			t.SourceSystem,
			t.ListDate,
			t.ClientOpenDate, 
			t.ClientScore,
			t.StatusCode,
			t.CancelCode,
			t.OriginalCancelCode,
			t.CancelDate,
			t.ChargeOffDate,
			t.IsAccountWorked,
			t.InsertDate,
			t.UpdateDate,
			t.ConsumerId,
			t.CustomerMatchId,
			t.KeySourceSystem,
			t.KeyClient,
			t.IDL_Date,
			t.LeadAccount,
			t.OfficeID,
			h.StatusCode, 
			t.FirstName, 
			t.LastName, 
			t.CustomerState
           

	DROP TABLE IF EXISTS #cur_restricted 

	/*
		GET CURRENTLY RESTRICTED CLIENT THAT DON'T HAVE PREVIOUS RESTRICTIONS 
	*/
	SELECT distinct h.*
	INTO #cur_restricted 
	FROM #dimcustomer_restricted t 
		JOIN DW_MSTR_DM.dbo.DimCustomer h WITH (NOLOCK) ON h.KeyCustomer = t.KeyCustomer 
								and h.StatusCode = t.StatusCode 
		LEFT JOIN #dimcustomer_previous_restriction p ON p.KeyCustomer = h.KeyCustomer 
	WHERE p.KeyCustomer IS NULL 
		--AND h.UpdateDate >= '2023-03-01' 
	   

	DROP TABLE IF EXISTS #temp_restricted_called
  
	/*
		COLLECT CALLS FOR EACH DAY AFTER RESTRICTION DATE 
	*/ 
	SELECT ch.KeyCustomer, ch.CustomerId, ch.ClientId, ch.SourceSystem, ch.ListDate, ch.FirstName, ch.LastName, ch.CustomerState, ch.StatusCode, ch.DateRestricted, 
		fcc.KeyDate_CallDate, 
		COUNT(1) CountOfExceptions   
	INTO #temp_restricted_called  
	FROM DW_MSTR_DM.dbo.FactCustomerCall fcc WITH (NOLOCK) 
		JOIN (SELECT KeyCustomer, CustomerId, ClientId, SourceSystem, ListDate, FirstName, LastName, CustomerState, StatusCode, UpdateDate DateRestricted  
			FROM #cur_restricted  
				UNION 
			SELECT KeyCustomer, CustomerId, ClientId, SourceSystem, ListDate, FirstName, LastName, CustomerState, StatusCodeHist StatusCode, InsertDateHist DateRestricted  
			FROM #dimcustomer_previous_restriction ) ch on ch.KeyCustomer = fcc.KeyCustomer  
	WHERE fcc.KeyDate_CallDate > CONVERT(VARCHAR,CAST(@max_excpt_date AS DATE),112)
			AND fcc.KeyDate_CallDate > CONVERT(VARCHAR,CAST(ch.DateRestricted AS DATE),112) 
	GROUP BY ch.KeyCustomer, ch.CustomerId, ch.ClientId, ch.SourceSystem, ch.ListDate, ch.FirstName, ch.LastName, ch.CustomerState, ch.StatusCode, ch.DateRestricted, fcc.KeyDate_CallDate
     
	/*
		REPORT NEW EXCEPTIONS 
	*/
	INSERT INTO [CLIENT_ANALYTICS].[dbo].[RPT_CFPB_excpt_resticted_called] 
	(  
		[KeyCustomer]
		,[CustomerId]
		,[ClientId]
		,[SourceSystem]
		,[ListDate]
		,[DateRestricted]
		,[StatusCode]
		,[FirstName]
		,[LastName]
		,[CustomerState]
		,[ExceptionDate]  
		,[CountOfExceptions]							
	)
	SELECT t.[KeyCustomer]
		,t.[CustomerId]
		,t.[ClientId]
		,t.[SourceSystem]
		,t.[ListDate] 
		,t.[DateRestricted]
		,t.[StatusCode]
		,t.[FirstName]
		,t.[LastName]
		,t.[CustomerState]  
		, DATEFROMPARTS(t.KeyDate_CallDate / 10000, t.KeyDate_CallDate / 100 % 100, t.KeyDate_CallDate % 100) ExceptionDate
		,t.[CountOfExceptions]   
	FROM #temp_restricted_called t 
		LEFT JOIN [CLIENT_ANALYTICS].[dbo].[RPT_CFPB_excpt_resticted_called] r ON r.KeyCustomer =  t.KeyCustomer 
																				AND r.DateRestricted = t.DateRestricted  
																				AND r.ExceptionDate = DATEFROMPARTS(t.KeyDate_CallDate / 10000, t.KeyDate_CallDate / 100 % 100, t.KeyDate_CallDate % 100)
	WHERE t.KeyDate_CallDate >= CONVERT(VARCHAR,CAST(@max_excpt_date AS DATE),112)  
			AND r.KeyCustomer IS NULL 
	ORDER BY t.DateRestricted    
  
END ;