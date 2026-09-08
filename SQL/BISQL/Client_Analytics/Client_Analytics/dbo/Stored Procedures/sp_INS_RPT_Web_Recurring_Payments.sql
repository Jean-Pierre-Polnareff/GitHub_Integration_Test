



-- =============================================
-- Object: dbo.sp_INS_RPT_Web_Recurring_Payments
-- Create date: 01/11/2024
--
--  Description: Inserts data for Web Recurring Payments Program from [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] into [Client_Analytics].[dbo].[RPT_Web_Portal_Recurring_Payments]
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	01/11/2024	 Created
-- =============================================

CREATE PROCEDURE [dbo].[sp_INS_RPT_Web_Recurring_Payments] 
   @startdate DATE = NULL
AS


BEGIN
	SET NOCOUNT ON;

	SET @startdate = ISNULL(@startdate,DATEADD(dd,-7,CAST(GETDATE() AS DATE))); 



  ----------------------------------------------------------Remove records for last 7 days--------------------------------------------------------------------------------
	DELETE FROM CLIENT_ANALYTICS.dbo.RPT_Web_Portal_Recurring_Payments
	WHERE CAST(CapturedOn AS DATE) >= @startdate
	;



  ---------------------------------------------------Get Recurring Payments data for last 7 days--------------------------------------------------------------------------
  DROP TABLE IF EXISTS #recur_pymt
  SELECT * 
  INTO #recur_pymt 
  FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
  where [key] = 'reocurring_submitted'
     AND CAST(CapturedOn AS DATE) >= @startdate

  -------------------------------------------Get Initial Balance for all the accounts with Recurring Payments-------------------------------------------------------------
  DROP TABLE IF EXISTS #recur_pymt_bal
  SELECT rp.*
       , COALESCE(dcu.InitialBalance, obf.INITIAL_BALANCE, 0) [Balance] 
  INTO #recur_pymt_bal
  FROM #recur_pymt rp
  LEFT JOIN DW_MSTR_DM.dbo.DimCustomer dcu WITH (NOLOCK)
  ON rp.KeyCustomer = dcu.KeyCustomer 
    AND rp.KeySourceSystem = dcu.KeySourceSystem
  LEFT JOIN DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT obf WITH (NOLOCK)
  ON rp.CustomerID = obf.CUSTOMER_ID 
     AND rp.KeySourceSystem = 4

  --SELECT * FROM #recur_pymt_bal where balance = 0

  -----------------------------------------------Make sure the payment method is only reocurring_submitted----------------------------------------------------------------
   DROP TABLE IF EXISTS #recur_pymt_method
  SELECT SessionID
       , Payments
       , [key] 
       , LAG([Key] ,2) OVER(PARTITION BY SessionID ORDER BY MetricID) AS  Payment_Type
  INTO #recur_pymt_method
  FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] WITH (NOLOCK)
  WHERE SessionId
  IN
  (
  SELECT DISTINCT SessionID 
  FROM #recur_pymt_bal
  )  


  -------------------------------Insert the data into reporting table [CLIENT_ANALYTICS].[dbo].[RPT_Web_Portal_Recurring_Payments]----------------------------------------
  INSERT INTO [CLIENT_ANALYTICS].[dbo].[RPT_Web_Portal_Recurring_Payments]
  (
  [MetricId]
      ,[Key]
      ,[CapturedOn]
      ,[Count]
      ,[Description]
      ,[ReferenceNumber]
      ,[Payments]
      ,[ClientId]
      ,[ClientParent]
      ,[PDC]
      ,[Posted]
      ,[IpAddress]
      ,[SessionId]
      ,[QueryString]
      ,[UTM_Source]
      ,[UTM_Campaign]
      ,[SourceSystem]
      ,[KeySourceSystem]
      ,[KeyCustomer]
      ,[KeyClient]
      ,[CustomerID]
      ,[KeyETLAuditHistory_Inserted]
      ,[KeyETLAuditHistory_Last_Updated]
      ,[InsertDate]
      ,[UpdateDate]
      ,[OneTimePaymentDate]
      ,[OneTimePaymentAmount]
      ,[PaySeriesStart]
      ,[PaySeriesFrequency]
      ,[PaySeriesAmount]
      ,[PaySeriesCount]
      ,[PaySumTotal]
      ,[DeviceCategory]
      ,[OS]
      ,[Browser]
	  ,[Balance]
	  ,[Payment$]
	  ,[Payment_Method]
	  ,[Min-Pay Tiers]
	  )
 SELECT rpb.*
       , rpm.Payments AS Payment$
       , rpm.[Payment_Method] 
	   --, CASE WHEN  CAST(rpb.CapturedOn  AS DATE) >= '2025-07-11' THEN '2.5% floor'
	   --  ELSE
	   --  CASE WHEN Balance * 0.025 < 50 THEN '2.5% floor' ELSE '5.0% floor' END 
	   --  END AS [Min-Pay Tiers]
	   , [Min-Pay Tiers] = '2.5% floor'                                        ----------------As per TedM, 5% floor offer has been retired effective 7/11/2025
  FROM #recur_pymt_bal rpb 
  LEFT JOIN 
  (SELECT SessionID
        , Payments
        , [key] AS [Payment_Method]
        , Payment_Type 
  FROM #recur_pymt_method 
  WHERE
    Payment_Type = 'reocurring_submitted'
  --AND [Payment_Method] IN ('checking_account_used', 'debit_card_used', 'credit_card_used')
  )rpm
  ON rpb.SessionId = rpm.SessionId
  where  PaySeriesStart IS NOT NULL  
      OR  rpm.Payments <> 0.00 
 ;



 END;