
 
CREATE   PROCEDURE [dbo].[usp_RPT_fact_letter_idn_excpt] 

AS 
BEGIN 
                   
	DROP TABLE IF EXISTS #TEMP_LTR_50_DAYS 

	SELECT ltr.*,lt.[LETTERTYPE] 
	INTO #TEMP_LTR_50_DAYS 
	FROM [DW_MSTR_DM].[dbo].[FactCustomerLetter] ltr WITH (NOLOCK) 
		JOIN [DW_MSTR_DM].[dbo].[DimLetterType] lt WITH (NOLOCK) on ltr.[KeyLetterType]=lt.[KeyLetterType]
	WHERE (lt.[LETTERTYPE] LIKE 'IDN%' 
			OR lt.[LETTERTYPE] LIKE 'EM%') 
			AND ltr.KeyDate_MailDate >= CONVERT(VARCHAR,DATEADD(dd,-50,CAST(GETDATE() AS DATE)),112)  

	DROP TABLE IF EXISTS #TEMP_LTR
  
	SELECT	
			dcu.[CustomerId], 
			dcu.[ConsumerId],
			dcl.[ClientId], 
			dcl.[ClientStream], 
			dcu.[IDL_Date],    
			t.KeyCustomerLetter,
			t.LetterKey, 
			t.[LetterType], 
			CASE WHEN b.LTR IS NULL THEN 'PAPER' ELSE 'EMAIL' END AS [TypeFlg], 
			CAST(CONVERT (datetime,convert(char(8),t.[KeyDate_FileDate])) AS date) ReqDate, 
			dcu.[ListDate], 
			dcu.[StatusCode],       
			dcu.[CancelCode],
			dcl.[LocationWorked],
			dcu.[InitialBalance],  
			dcu.[LastPaymentDate], 
			dcu.[CurrentBalance],   
			dcu.[CustomerState], 
			dcu.KeySourceSystem, 
			dcu.FirstName + ' ' + dcu.LastName NameOnAccount, 
			src.[SourceSystem2] 
	INTO #TEMP_LTR 
	FROM [DW_MSTR_DM].[dbo].[DimCustomer] dcu WITH (NOLOCK) 
		LEFT JOIN #TEMP_LTR_50_DAYS t WITH (NOLOCK) ON t.KeyCustomer = dcu.KeyCustomer 
		LEFT JOIN [DW_MSTR_DM].[dbo].[DimClient] dcl WITH (NOLOCK) on dcu.[KeyClient]=dcl.[KeyClient] 
		LEFT JOIN [CLIENT_ANALYTICS].[dbo].[RPT_email_guid] b WITH (NOLOCK) on dcu.CustomerId=b.customerid 
											and dcl.[ClientId] = b.[ClientId] 
											and CAST(CONVERT (datetime,convert(char(8),t.[KeyDate_FileDate])) AS date) = b.send_date 
											and t.[LetterType] = b.ltr  
		LEFT JOIN [DW_MSTR_DM].[dbo].[DimSourceSystem] src WITH (NOLOCK) on t.[KeySourceSystem] = src.[KeySourceSystem]
	WHERE (t.[LETTERTYPE] LIKE 'IDN%' 
			OR t.[LETTERTYPE] LIKE 'EM%' 
			OR dcu.IDL_Date IS NOT NULL)            
		AND (
				CAST(dcu.IDL_Date AS DATE) >= DATEADD(dd,-50,CAST(GETDATE() AS DATE)) 
					OR t.KeyDate_MailDate >= CONVERT(VARCHAR,DATEADD(dd,-50,CAST(GETDATE() AS DATE)),112) 
			) 


	DROP TABLE IF EXISTS #temp_ltr_partitioned

	SELECT 
		CASE WHEN LEAD(CustomerId, 1, NULL) OVER (ORDER BY CustomerId, ReqDate) = CustomerId 
		THEN 
			LEAD(LetterType, 1, NULL) OVER (ORDER BY CustomerId, ReqDate) 
			ELSE NULL 
		END NEXT_LetterType, 
		CASE WHEN LEAD(CustomerId, 1, NULL) OVER (ORDER BY CustomerId, ReqDate) = CustomerId 
		THEN  
			LEAD(ReqDate, 1, NULL) OVER (ORDER BY CustomerId, ReqDate) 
			ELSE NULL 
		END NEXT_ReqDate, 
		CASE WHEN LEAD(CustomerId, 1, NULL) OVER (ORDER BY CustomerId, ReqDate) = CustomerId 
		THEN  
			LEAD(LetterKey, 1, NULL) OVER (ORDER BY CustomerId, ReqDate) 
			ELSE NULL 
		END NEXT_LetterKey,
		* 
	INTO #temp_ltr_partitioned 
	FROM #temp_ltr 
	ORDER BY customerid, ReqDate  
 

	INSERT INTO CLIENT_ANALYTICS.[dbo].[fact_letter_idn_excpt]
			   ([ltr_expt_id]
			   ,[CustomerId]
			   ,[ConsumerId]
			   ,[StatusCode]
			   ,[CancelCode]
			   ,[ListDate] 
			   ,[ClientId]  
			   ,[ClientStream]
			   ,[KeySourceSystem]
			   ,[SourceSystem2]
			   ,[IDN_Date]
			   ,[NameOnAccount]
			   ,[InitialBalance]
			   ,[Letter ID]       
			   ,[ReqDate]
			   ,[LetterType]
			   ,[TypeFlg]
			   ,[CustomerState]
			   ,[Days Since IDN])
	SELECT        
		2 ltr_expt_id,
		CustomerId ,                         
		ConsumerId, 
		StatusCode, 
		CancelCode, 
		ListDate,
		ClientId, 
		ClientStream, 
		KeySourceSystem,
		SourceSystem2,
		CASE WHEN IDL_Date IS NULL AND LetterType LIKE 'IDN%' THEN ReqDate ELSE IDL_Date END [IDN_Date], 
		NameOnAccount,
		InitialBalance, 
		Next_LetterKey [Letter ID], 
		NEXT_ReqDate ReqDate, 
		NEXT_LetterType LetterType,  
		TypeFlg, 
		CustomerState, 
		DATEDIFF(DD,CAST(ReqDate AS DATE),CAST(Next_ReqDate AS DATE)) [Days Since IDN]  
	FROM #temp_ltr_partitioned t  
	WHERE ( 
			LETTERTYPE LIKE 'IDN%' 
				AND ( (Next_LetterType LIKE 'EM%'    
					AND DATEDIFF(dd,ReqDate,Next_ReqDate) <= 50))
		) 

END