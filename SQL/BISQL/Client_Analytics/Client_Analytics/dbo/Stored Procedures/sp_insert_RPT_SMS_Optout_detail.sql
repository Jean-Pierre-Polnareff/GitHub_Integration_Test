USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_RPT_SMS_Optout_detail]    Script Date: 8/29/2023 9:23:59 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



alter PROCEDURE [dbo].[sp_insert_RPT_SMS_Optout_detail] 

AS


/*
Object: sp_insert_fact_dial_excpt_client

Description: Identify and insert SMS OptOuts into [CLIENT_ANALYTICS].[dbo].[RPT_SMS_Optout_detail]

--IMPORTANT NOTE: This stored procedure gets called by [CLIENT_ANALYTICS].[dbo].[sp_insert_RPT_SMS_Optout_Excpt] and cannot be executed individually. 
--                

Author			Date		Description
Amod Ramugade	08/29/2023	Created

-- =============================================
*/

BEGIN
	SET NOCOUNT ON;


------------------------SMS Optouts for last 31 days------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #SMS_Optouts
       SELECT s.ClientId
		   , s.KeyCustomer
		   , s.CustomerId
		   , s.KeySourceSystem
		   , s.SourceSystem
		   , s.CallStartTime
		   , s.WeekId
		   , s.DialedPhoneNumber
		   , s.SessionId  
		   , s.ClientParent
		   , s.SMS_optout_date
		   INTO #SMS_Optouts
		   FROM #SMS s
		WHERE livevox_result  LIKE '%Stop%'
			----9/20/22 As per TedM, excluding Bob's customerid for thirdprod
			AND NOT (s.CustomerId = 102231 AND s.KeySourceSystem = 2)			
			---10/3 As per TedM, excluding Test customer IDs for Artiva and HTI customer
			AND NOT (s.CustomerId in ('11644','1224851','1687','1028999','14892599') 
			and s.KeySourceSystem in (1,2,3)) 



-------------------------------------------insert SMS optouts into client_analytics.dbo.RPT_SMS_Optout_detail-------------------------------------------

INSERT INTO [CLIENT_ANALYTICS].dbo.[RPT_SMS_Optout_detail](
	[ClientId] 
	,[KeyCustomer] 
	,[CustomerId] 
	,[KeySourceSystem] 
	,[SourceSystem] 
	,[CallDate] 
	,[CallStartTime] 
	,[WeekId] 
	,[DialedPhoneNumber] 
	,[SessionId] 
	,[ClientParent] 
	,[SMS_optout_date] 
	,[insert_date]  
	)
SELECT   so.ClientId 
	   , so.KeyCustomer
	   , so.CustomerId
	   , so.KeySourceSystem
	   , so.SourceSystem
 	   , cast(so.CallStartTime as date) as CallDate
	   , so.CallStartTime 
	   , so.WeekId
	   , so.DialedPhoneNumber
	   , so.SessionId
	   , so.ClientParent
 	   , so.SMS_optout_date
	   , insert_date = getdate()
FROM #SMS_Optouts so
	LEFT JOIN CLIENT_ANALYTICS.dbo.RPT_SMS_Optout_detail sod (NOLOCK) 
	ON so.SessionId = sod.SessionId
WHERE sod.SessionId IS NULL



END;


GO