

-- =============================================
-- Object: dbo.usp_Insert_RPT_Web_Waterfall_AXP_Payment_Program_Detail
-- Create date: 03/02/2023
--
--  Description: Inserts data for AXP Payment Program from [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] into [Client_Analytics].[dbo].[RPT_Web_Waterfall_AXP_Payment_Program_Detail]
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	03/02/2023	 Created
--	Amod Ramugade	03/04/2024	 Modified
-- =============================================

CREATE PROCEDURE [dbo].[usp_Insert_RPT_Web_Waterfall_AXP_Payment_Program_Detail] 

AS

SET NOCOUNT ON;



DROP TABLE IF EXISTS #t
SELECT a.*, Campaign =  'Enrolled', Campaign_Order = 2  
---,CASE WHEN CHARINDEX('O', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end Oasis  
---,CASE WHEN CHARINDEX('F', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end FRP  
---,CASE WHEN CHARINDEX('A', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end Apollo
---,CASE WHEN CHARINDEX('R', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end ReAge   
---,CASE WHEN CHARINDEX('C', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end Care
---,CASE WHEN CHARINDEX('N', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end NCO
---,CASE WHEN CHARINDEX('T', a.[Description]) = 0 AND  [key] = 'program_enrollment' then 0 else 1 end Reinstatement
INTO #t
FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] a (NOLOCK)
 JOIN
 (
  SELECT  DISTINCT SessionId FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
where
 [key] = 'program_enrollment'
 AND SessionId IS NOT NULL
) Enrolled
on a.SessionId = Enrolled.SessionId


----------- Apollo and Oasis programs were decommissioned starting 12/4/2023
----------- Apollo --> Reinstatement
----------- Oasis --> NCO

/*
SELECT * FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
WHERE [Key] = 'program_enrollment' AND [Description] = 'Reinstatement'
--2023-12-15 22:12:21

SELECT * FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
WHERE [Key] = 'program_enrollment' AND [Description] = 'NCO'
--2023-12-04 21:28:37
*/


UPDATE #t 
SET [Description] = REPLACE([Description], 'A', 'T')
WHERE 
 [Key] = 'program_offers_list'
AND
 CAST(CapturedOn AS DATE) >= '2023-12-04'
AND
 [Description] LIKE '%A%'

/*
 SELECT * FROM #t
WHERE 
 [Key] = 'program_offers_list'
AND
 CAST(CapturedOn AS DATE) >= '2023-12-04'
AND
 [Description] LIKE '%T%'
order by MetricId
*/

UPDATE #t 
SET [Description] = REPLACE([Description], 'O', 'N')
WHERE 
 [Key] = 'program_offers_list'
AND
 CAST(CapturedOn AS DATE) >= '2023-12-04'
AND
 [Description] LIKE '%O%'


/*
SELECT * FROM #t
WHERE 
 [Key] = 'program_offers_list'
AND
 CAST(CapturedOn AS DATE) >= '2023-12-04'
AND
 [Description] LIKE '%N%'
order by MetricId
*/

DROP TABLE IF EXISTS #t2
 SELECT a.*, Campaign =  'Viewed', Campaign_Order = 1  
,CASE WHEN CHARINDEX('O', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end Oasis  
,CASE WHEN CHARINDEX('F', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end FRP  
,CASE WHEN CHARINDEX('A', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end Apollo
,CASE WHEN CHARINDEX('R', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end ReAge  
,CASE WHEN CHARINDEX('C', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end Care
,CASE WHEN CHARINDEX('N', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end NCO
,CASE WHEN CHARINDEX('T', a.[Description]) = 0 AND  [key] = 'program_offers_list' then 0 else 1 end Reinstatement
--,Program_Type = NULL
INTO #t2
FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] a (NOLOCK)
 JOIN
 (
  SELECT   SessionId, min(CapturedOn) CapturedOn FROM [DW_MSTR_DM].[dbo].[Web_Waterfall_Metrics_Data_Client] (NOLOCK)
where
 [key] = 'program_offers_list'
 AND SessionId IS NOT NULL
 AND [Description] IS NOT NULL
 GROUP BY SessionId
) Offers
on a.SessionId = Offers.SessionId
AND a.CapturedOn = Offers.CapturedOn
AND a. [key] = 'program_offers_list'

----------- Apollo and Oasis programs were decommissioned starting 12/4/2023
----------- Apollo --> Reinstatement
----------- Oasis --> NCO
	
UPDATE #t2 
SET Reinstatement = 1, Apollo = 0,  [Description] = REPLACE([Description], 'A', 'T')
WHERE 
 [Key] = 'program_offers_list'
AND
 CAST(CapturedOn AS DATE) >= '2023-12-04'
AND
 [Description] LIKE '%A%'


UPDATE #t2 
SET NCO = 1, Oasis = 0, [Description] = REPLACE([Description], 'O', 'N')
 WHERE 
 [Key] = 'program_offers_list'
AND
 CAST(CapturedOn AS DATE) >= '2023-12-04'
AND
 [Description] LIKE '%O%'


DROP TABLE IF EXISTS #final
SELECT #t.*
, Program_Type.[Description] AS Program_Type 
,Occurrence = 1
INTO #final
FROM #t
LEFT JOIN
(
Select DISTINCT SessionId, [Description] FROM #t
where
 [key] = 'program_enrollment'
 AND SessionId IS NOT NULL
 )Program_Type
 ON #t.SessionId = Program_Type.SessionId
 

 UNION
 
 SELECT [MetricId]
      , [Key]
      , [CapturedOn]
      , [Count]
      , [Description]
      , [ReferenceNumber]
      , [Payments]
      , [ClientId]
      , [ClientParent]
      , [PDC]
      , [Posted]
      , [IpAddress]
      , [SessionId]
      , [QueryString]
      , [UTM_Source]
      , [UTM_Campaign]
      , [SourceSystem]
      , [KeySourceSystem]
      , [KeyCustomer]
      , [KeyClient]
      , [CustomerID]
      , [KeyETLAuditHistory_Inserted]
      , [KeyETLAuditHistory_Last_Updated]
      , [InsertDate]
      , [UpdateDate]
      , [OneTimePaymentDate]
      , [OneTimePaymentAmount]
      , [PaySeriesStart]
      , [PaySeriesFrequency]
      , [PaySeriesAmount]
      , [PaySeriesCount]
      , [PaySumTotal]
      , [DeviceCategory]
      , [OS]
      , [Browser]
	  , [Campaign]
	  , [Campaign_Order]
	  , [Program_Type]
	  , [Occurrence] 
FROM   
(
 SELECT [MetricId]
      , [Key]
      , [CapturedOn]
      , [Count]
      , [Description]
      , [ReferenceNumber]
      , [Payments]
      , [ClientId]
      , [ClientParent]
      , [PDC]
      , [Posted]
      , [IpAddress]
      , [SessionId]
      , [QueryString]
      , [UTM_Source]
      , [UTM_Campaign]
      , [SourceSystem]
      , [KeySourceSystem]
      , [KeyCustomer]
      , [KeyClient]
      , [CustomerID]
      , [KeyETLAuditHistory_Inserted]
      , [KeyETLAuditHistory_Last_Updated]
      , [InsertDate]
      , [UpdateDate]
      , [OneTimePaymentDate]
      , [OneTimePaymentAmount]
      , [PaySeriesStart]
      , [PaySeriesFrequency]
      , [PaySeriesAmount]
      , [PaySeriesCount]
      , [PaySumTotal]
      , [DeviceCategory]
      , [OS]
      , [Browser]
	  , Campaign
	  , Campaign_Order
	  , Apollo, FRP, ReAge, Oasis, Care, NCO, Reinstatement 
   FROM #t2
   ) p  
UNPIVOT  
   (Occurrence FOR Program_Type IN   
      (Apollo, FRP, ReAge, Oasis, Care, NCO, Reinstatement)  
)AS unpvt
where Occurrence > 0;  


TRUNCATE TABLE Client_Analytics.dbo.RPT_Web_Waterfall_AXP_Payment_Program_Detail
INSERT INTO Client_Analytics.dbo.RPT_Web_Waterfall_AXP_Payment_Program_Detail
SELECT f.* , dcs.InitialBalance
---, pymt.pymt_date, pymt.total_payments, pymt.total_collections
---,(ISNULL(dcs.InitialBalance,0) - ISNULL(pymt.total_collections, 0)) AS currentbal_during_arrangements
FROM #final f
left join
DW_MSTR_DM.dbo.DimCustomer dcs (NOLOCK)
--on f.CustomerID = dcs.CustomerId
on f.KeyCustomer  = dcs.KeyCustomer
AND f.KeySourceSystem  = dcs.KeySourceSystem
---left join
---CLIENT_ANALYTICS.dbo.RPT_payment_detail pymt
---on f.CustomerID  = pymt.CUSTOMER_ID
---AND pymt.crm = 'AMEX Latitude'
---AND pymt.pymt_date <= CAST(DATEADD(day,2,f.capturedon) AS DATE)
and f.Campaign = 'Enrolled'