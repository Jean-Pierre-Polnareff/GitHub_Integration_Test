





/*
-- =============================================
-- Object: dbo.sp_INS_RPT_Web_Waterfall_Metrics_Data_Client_Summary
-- Create date: 7/30/2024
--
--  Description: Inserts data for Metrics Data required for Web Waterfall PBI reporting 
--               from [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] into [Client_Analytics].[dbo].[RPT_Web_Waterfall_Metrics_Data_Client_Summary]
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	8/1/2024	 Created
-- =============================================
*/

CREATE PROCEDURE [dbo].[sp_INS_RPT_Web_Waterfall_Metrics_Data_Client_Summary] 
	@startdate DATE = NULL

AS

BEGIN
	SET NOCOUNT ON;

	--DECLARE  @startdate DATE  =  DATEADD(dd,-7,CAST(GETDATE() AS DATE));   --'2019-01-01'   
 SET @startdate = ISNULL(@startdate,DATEADD(dd,-7,CAST(GETDATE() AS DATE))); 

 DELETE FROM CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_Metrics_Data_Client_Summary
	WHERE CAST(CapturedOn AS DATE) >= @startdate
	;


	DROP TABLE IF EXISTS #Web_Waterfall_Metrics_Data_Client
	SELECT * 
	INTO #Web_Waterfall_Metrics_Data_Client
	FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
	WHERE CAST(CapturedOn AS DATE) >= @startdate
	AND [IpAddress] NOT IN ('100.34.50.164', '34.195.158.146', '100.34.69.218');

	CREATE INDEX #combo1 ON #Web_Waterfall_Metrics_Data_Client(CustomerID,ClientId,SourceSystem);
	

        

DROP TABLE IF EXISTS #ClientMapping
 SELECT DISTINCT
       [ClientId]
     --,[ClientParent]
	  ,[ClientParentGroup] AS ClientParent
      ,[ClientStreamId]
      ,[ClientStream]
	--  ,CASE WHEN [SourceSystem] = 'MEDPROD' THEN 'MedProd Artiva'
 -- WHEN SourceSystem = 'THIRDPROD' THEN 'ThirdProd Artiva'
	--ELSE [SourceSystem]
 -- END AS 
    , [SourceSystem]
	INTO #ClientMapping  FROM [DW_MSTR_DM].[dbo].[DimClient] (NOLOCK)

	  UNION 

	  SELECT DISTINCT
       [Client_ID] AS ClientId
      ,[Parent] AS ClientParent
      ,CONVERT( VARCHAR(50) ,[Client_Stream_ID]) AS ClientStreamId
      ,[Client_Stream] AS ClientStream
	  ,[SourceSystem] = 'FACS'
	  FROM [DW_MSTR_DM].[dbo].[TblClientStreams] (NOLOCK)

CREATE INDEX #combo2 ON #ClientMapping(ClientId,SourceSystem);


DROP TABLE IF EXISTS #dcp
SELECT
a.CustomerId 
----CAST(a.CustomerId AS VARCHAR (10)) AS CustomerId
  ,dss.SourceSystem
  ,a.Clientid
  ,a.ProductType
  ,b.Product
  INTO #dcp
  FROM [DW_MSTR_DM].[dbo].[DimCustomerProduct] (NOLOCK) a
  LEFT JOIN
  [CLIENT_ANALYTICS].[dbo].[RPT_Amex_ProductType-Product_Mapping] (NOLOCK) b
  ON a.ProductType = b.ProductType
  LEFT JOIN [DW_MSTR_DM].[dbo].[DimSourceSystem] dss (nolock)
  on dss.KeySourceSystem = a.KeySourceSystem

CREATE INDEX #combo3 ON #dcp(CustomerId,ClientId,SourceSystem);


DROP TABLE IF EXISTS #t
SELECT w.[MetricId]
      , CASE WHEN w.[Key] IN ('alt_pin_login_failed', 'ssn_login_failed', 'dob_login_failed') THEN 'pin_login_failed' 
             ELSE w.[Key]
             END  AS [Key]
      ,CAST(w.[CapturedOn] AS DATE) AS [CapturedOn]
      ,w.[Count]
      ,w.[Description]
      ,w.[ReferenceNumber]
      ,w.[Payments]
      ,ISNULL(w.[ClientId], '') AS [ClientId]
      ,w.[ClientParent]
      ,w.[PDC]
      ,w.[Posted]
      ,w.[IpAddress]
      ,w.[SessionId]
      ,w.[QueryString]
      ,w.[UTM_Source]
      ,w.[UTM_Campaign]
      ,w.[SourceSystem]
      --,w.[KeySourceSystem]
      --,w.[KeyCustomer]
      --,w.[KeyClient]
      ,w.[CustomerID]
      --,w.[KeyETLAuditHistory_Inserted]
      --,w.[KeyETLAuditHistory_Last_Updated]
      --,w.[InsertDate]
      --,w.[UpdateDate]
      ,w.[OneTimePaymentDate]
      ,w.[OneTimePaymentAmount]
      ,w.[PaySeriesStart]
      ,w.[PaySeriesFrequency]
      ,w.[PaySeriesAmount]
      ,w.[PaySeriesCount]
      ,w.[PaySumTotal]
      ,w.[DeviceCategory]
      ,w.[OS]
      ,w.[Browser]
      ,dss.SourceSystem AS [CRM]
      ,CASE WHEN  w.[KeySourceSystem] = 0 OR w.[KeySourceSystem] IS NULL THEN 'Pre-Login'
            ELSE 'Post-Login'
            END AS  [Login Type] 
      ,CASE WHEN  w.[KeySourceSystem] = 0 OR w.[KeySourceSystem] IS NULL THEN 'Unauthenticated'
            ELSE 'Authenticated'
            END AS  [Authenticated/Unauthenticated] 
      ,w.[CapturedOn] AS [Captured DateTime]
      ,CASE WHEN cm.ClientParent IS NULL THEN ''
             ELSE cm.ClientParent
             END AS [Client]
      ,ISNULL(cm.[ClientParent],'') AS [Parent]
      ,ISNULL(cm.ClientStreamId, '') AS [ClientStreamId]
      ,ISNULL(cm.ClientStream, '') AS[ClientStream]
      ,CAST(w.[CustomerID] AS VARCHAR(20)) [CustomerID(Varchar)]
      ,dcp.ProductType
      ,dcp.Product
    INTO #t
  FROM #Web_Waterfall_Metrics_Data_Client w
  LEFT JOIN #ClientMapping cm
  ON w.ClientId = cm.ClientId
  AND w.SourceSystem = cm.SourceSystem
  LEFT JOIN #dcp dcp
  ON w.CustomerID = dcp.CustomerId
  AND w.SourceSystem = dcp.SourceSystem
    LEFT JOIN [DW_MSTR_DM].[dbo].[DimSourceSystem] dss (nolock)
  on dss.KeySourceSystem = w.KeySourceSystem
WHERE CAST(w.[CapturedOn] AS DATE) >= @startdate 
AND w.[IpAddress] NOT IN ('100.34.50.164', '34.195.158.146', '100.34.69.218')



INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Web_Waterfall_Metrics_Data_Client_Summary 
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
      ,[CustomerID]
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
      ,[CRM]
      ,[Login Type]
      ,[Authenticated/Unauthenticated]
      ,[Captured DateTime]
      ,[Client]
      ,[Parent]
      ,[ClientStreamId]
      ,[ClientStream]
      ,[CustomerID(Varchar)]
      ,[ProductType]
      ,[Product]
	  )
SELECT 
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
      ,[CustomerID]
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
      ,[CRM]
      ,[Login Type]
      ,[Authenticated/Unauthenticated]
      ,[Captured DateTime]
      ,[Client]
      ,[Parent]
      ,[ClientStreamId]
      ,[ClientStream]
      ,[CustomerID(Varchar)]
      ,[ProductType]
      ,[Product]
FROM #t 
;

END;