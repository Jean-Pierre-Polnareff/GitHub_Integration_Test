
ALTER PROCEDURE [dbo].[sp_insert_RPT_CTNV_transfers]
@start DATE=null
AS
/* 
Object: sp_insert_RPT_CTNV_transfers

Description: Remove last 15 call_dates from RPT_client_Amex_SMS_V2 and reinsert

Author			Date		Description
Mike Campbell	11/9/2022	Created 
*/

BEGIN
	SET NOCOUNT ON;
	 
	DELETE 
	FROM CLIENT_ANALYTICS.dbo.RPT_CTNV_transfers
	WHERE IB_Call_Date = isnull(@start, CAST(GETDATE() - 1 AS DATE));

	DROP TABLE IF EXISTS #ib;
		WITH areacode					
		AS
		(
			SELECT 203 AS AREACODE, 'CT' AS STATECODE UNION
			SELECT 475, 'CT' UNION
			SELECT 860, 'CT' UNION
			SELECT 959, 'CT' UNION
			SELECT 702, 'NV' UNION
			SELECT 725, 'NV' UNION
			SELECT 775, 'NV' 
		)


	SELECT a.StateCode as IB_state_from_areacode
		   , fcc.SessionId as IB_SessionId
		   , fcc.KeyCustomer as IB_KeyCustomer
		   , dcu.CustomerId as IB_CustomerId
		   , dcu.ClientId as IB_ClientId
		   , fcc.DialedPhoneNumber as IB_DialedPhoneNumber
		   , ddcc.Call_Center_Name as IB_Call_Center_Name
		   , dcs.Service_Name as IB_Service_name
		   , ddr.Dialer_Result as IB_Dialer_Result
		   , fcc.CallStartTime as IB_CallStartTime
		   , dd.CalendarDate as IB_Call_Date
	INTO #ib
	FROM dw_mstr_dm.dbo.FactCustomerCall fcc (nolock)
		   join areacode a on fcc.DialedAreaCode = a.AreaCode
		   join DW_MSTR_DM.dbo.dimdate dd (NOLOCK) ON fcc.KeyDate_CallDate = dd.KeyDate
		   join DW_MSTR_DM.dbo.DimDialerCallCenter ddcc (NOLOCK) ON fcc.KeyDialerCallCenter = ddcc.KeyDialerCallCenter
		   join DW_MSTR_DM.dbo.DimCrmService dcs (NOLOCK) ON fcc.KeyCrmService = dcs.KeyCrmService
		   join DW_MSTR_DM.dbo.DimDialerResult ddr (NOLOCK) ON fcc.KeyDialerResult = ddr.KeyDialerResult
		   join DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcc.KeyCustomer = dcu.KeyCustomer
	where dd.CalendarDate = ISNULL(@start,CAST(GETDATE() - 1 as date)) --'10/1/22'
		  AND ddr.Dialer_Result LIKE '%transfer%'
		  AND dcs.Service_Name NOT LIKE '%HTI%'


    DROP TABLE IF EXISTS #ib2
	SELECT i.*
		   , fcc.SessionId as Transfer_SessionId
		   , fcc.CallStartTime as Transfer_CallStartTime
		   , dcs.Service_Name as Transfer_Service_Name
		   , dcs.Service_ID as Transfer_Service_ID
	INTO #ib2
	FROM #ib i
		   JOIN DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK) ON i.IB_DialedPhoneNumber = fcc.DialedPhoneNumber
												  AND i.IB_SessionId != fcc.SessionId
												  AND datediff(minute,i.IB_CallStartTime,fcc.CallStartTime) BETWEEN 0 AND 5
           JOIN DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON fcc.KeyDate_CallDate = dd.KeyDate
		 		                               AND i.ib_call_date = dd.CalendarDate
		   JOIN DW_MSTR_DM.dbo.DimCrmService dcs (NOLOCK) ON fcc.KeyCrmService = dcs.KeyCrmService
	where dcs.Service_Name NOT LIKE '%HTI%'

    --stupid, but it's way faster to build above with inner joins and do left joins below as next step
	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_CTNV_transfers
	             (
				  IB_state_from_areacode,
                  IB_SessionId,
                  IB_KeyCustomer,
				  IB_CustomerId,
				  IB_ClientId,
				  IB_DialedPhoneNumber,
				  IB_Call_Center_Name,
				  IB_Service_name,
				  IB_Dialer_Result,
				  IB_CallStartTime,
				  IB_Call_Date,
				  Transfer_SessionId,
				  Transfer_CallStartTime,
				  Transfer_Service_ID,
				  Transfer_Service_Name,
				  InsertDate
				 )
	SELECT i.*
		   , i2.Transfer_SessionId
	       , i2.Transfer_CallStartTime
		   , i2.Transfer_Service_ID
		   , i2.Transfer_Service_Name
		   , getdate() as InsertDate
	FROM #ib i
		LEFT JOIN #ib2 i2 on i.IB_SessionId = i2.IB_SessionId

END;