




CREATE PROCEDURE [dbo].[sp_ins_RPT_Optin_Inventory_by_Month]
	

AS

BEGIN
	SET NOCOUNT ON;



		SELECT 
		E.KeySourceSystem,	
		E.ClientID,
		E.OptSource											Optin_Source_ID,
		dd.MonthDate as Optin_Month,
		SUM(CASE WHEN E.EmailStatus = 'Y' 
		AND IsNull(C.CurrentBalance,0) > 0
		AND IsNull(C.CustomerState,'') NOT IN ('NY','DC')
		AND ((E.OptinDate <= '2021-11-29' AND E.OptSource IN ('Consumer Phone','Consumer Portal','Consumer Written','Client'))
				OR
				(
					(E.OptinDate > '2021-11-29' AND E.KeySourceSystem IN (2,3) AND E.OptSource IN ('Consumer Phone','Consumer Portal','Consumer Written')) 
					OR
					(E.OptinDate > '2021-11-29' AND E.KeySourceSystem = 1      AND E.OptSource IN ('Consumer Phone','Consumer Portal','Consumer Written','Client'))
				)
			) THEN 1 ELSE 0 END)							Email_OptIns, 
		COUNT(*) Email_OptIns_All
		into #cen_em
		FROM DW_MSTR_DM.dbo.DimCustomerEmail E (NOLOCK) 
		JOIN DW_MSTR_DM.dbo.DimCustomer C (NOLOCK) ON E.KeyCustomer = C.KeyCustomer AND E.KeySourceSystem not in (0,4) 
		left join dw_mstr_dm.dbo.DimDate dd (nolock) on e.OPTINdate=dd.CalendarDate
		WHERE C.CancelDate IS NULL AND C.StatusCode <> 'DW_deactivate'
		GROUP BY 
		E.KeySourceSystem,
		E.ClientID,
		E.OptSource,
		dd.MonthDate;


		INSERT #cen_em(KeySourceSystem,ClientID,Optin_Source_ID,Optin_Month,Email_OptIns,Email_OptIns_All)
		SELECT 
		E.KeySourceSystem,	
		E.ClientID,
		E.OptSource											Optin_Source_ID,
		dd.MonthDate as Optin_Month,
		SUM(CASE WHEN E.EmailStatus = 'Y'
		AND IsNull(O.Principal_Balance,0) > 0
		AND IsNull(C.Customer_State,'') NOT IN ('NY','DC')
		AND ((E.OptinDate <= '2021-11-29' AND E.OptSource IN ('Consumer Phone','Consumer Portal','Consumer Written','Client'))
				OR
				(
					(E.OptinDate > '2021-11-29' AND S.Parent NOT IN ('Citibank','OneMain') AND E.OptSource IN ('Consumer Phone','Consumer Portal','Consumer Written'))
						OR
					(E.OptinDate > '2021-11-29' AND S.Parent     IN ('Citibank','OneMain') AND E.OptSource IN ('Consumer Phone','Consumer Portal','Consumer Written','Client'))
				)
			) THEN 1 ELSE 0 END)							Email_OptIns, 
		COUNT(*) Email_OptIns_All
		FROM DW_MSTR_DM.dbo.DimCustomerEmail E (NOLOCK) 
		JOIN DW_MSTR_DM.dbo.LU_Customer C (NOLOCK) ON E.CustomerID = C.Customer_ID AND E.KeySourceSystem = 4
		JOIN DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT O (NOLOCK) ON C.Customer_ID = O.Customer_ID
		JOIN DW_MSTR_DM.dbo.TblClientStreams S (NOLOCK) ON C.Client_ID = S.Client_ID
		left join DW_MSTR_DM.dbo.DimDate dd (nolock) on e.OPTINdate=dd.CalendarDate
		WHERE C.Cancel_Date IS NULL
		GROUP BY 
		E.KeySourceSystem,
		E.ClientID,
		E.OptSource,
		dd.MonthDate;


		SELECT 
		P.KeySourceSystem,
		P.ClientID,
		P.SMS_optin_source	Optin_Source_ID,
		dd.MonthDate as Optin_Month,
		COUNT(*)			SMS_OptIns 
		into #cen_sms
		FROM [DW_MSTR_DM].[dbo].[RadiusPhone] P (NOLOCK)
		JOIN [DW_MSTR_DM].[dbo].[DimCustomer] C (NOLOCK) ON P.KeyCustomer = C.KeyCustomer
		left join DW_MSTR_DM.dbo.DimDate dd (nolock) on p.SMS_optin_date=dd.CalendarDate
		WHERE P.SMS_optin_status = 'Y' AND C.CancelDate IS NULL AND C.StatusCode <> 'DW_deactivate' 
		AND IsNull(C.CurrentBalance,0) > 0
		AND IsNull(C.CustomerState,'') NOT IN ('NY','DC')
		AND P.SMS_optin_date > '2021-11-29'
		AND P.SMS_optin_Source IN ('Consumer Phone','Consumer Portal','Consumer Written','Client'/*,'Vendor'*/)
		GROUP BY 
		P.KeySourceSystem,
		P.ClientID,
		P.SMS_optin_source,											
		dd.MonthDate;


		INSERT #cen_sms(KeySourceSystem,ClientID,Optin_Source_ID,Optin_Month,SMS_OptIns)
		SELECT 
		4					KeySourceSystem,
		C.CLIENT_ID			ClientID,
		P.SMS_optin_source	Optin_Source_ID,
		dd.MonthDate as Optin_Month,
		COUNT(*)			SMS_OptIns 
		  FROM [DW_MSTR_DM].[dbo].[CUST_PHONE_HIST_SMS] P (NOLOCK)
		JOIN [DW_MSTR_DM].[dbo].[LU_Customer] C (NOLOCK) ON P.CUSTOMER_ID = C.CUSTOMER_ID
		JOIN DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT O (NOLOCK) ON C.Customer_ID = O.Customer_ID
		left join DW_MSTR_DM.dbo.DimDate dd (nolock) on p.SMS_optin_date=dd.CalendarDate
		WHERE P.SMS_optin_status = 'Y' AND C.Cancel_Date IS NULL
		AND IsNull(O.Principal_Balance,0) > 0
		AND IsNull(C.Customer_State,'') NOT IN ('NY','DC')
		AND P.SMS_optin_date > '2021-11-29'
		AND P.SMS_optin_Source IN ('Consumer Phone','Consumer Portal','Consumer Written','Client'/*,'Vendor'*/)
		GROUP BY 
		C.CLIENT_ID,
		P.SMS_optin_source,
		dd.MonthDate;

		truncate table client_analytics.dbo.RPT_Optin_Inventory_by_Month;

		insert into client_analytics.dbo.RPT_Optin_Inventory_by_Month
					(
					  KeySourceSystem
					  , ClientId
					  , Optin_Source_ID
					  , Optin_Month
					  , Email_OptIns
					  , Email_OptIns_All
					  , SMS_OptIns
					)
		select isnull(e.KeySourceSystem,s.KeySourceSystem) as KeySourceSystem
			   , isnull(e.clientid,s.clientid) as ClientId
			   , isnull(e.Optin_Source_ID,s.Optin_Source_ID) as Optin_Source_ID
			   , isnull(e.Optin_Month,s.Optin_Month) as Optin_Month
			   , sum(e.Email_OptIns) as Email_OptIns
			   , sum(e.Email_OptIns_All) as Email_OptIns_All
			   , sum(s.SMS_OptIns) as SMS_OptIns
		from #cen_em e
				full outer join
			 #cen_sms s on e.KeySourceSystem=s.KeySourceSystem
						   and e.ClientId=s.ClientID
						   and e.Optin_Month=s.Optin_Month
						   and e.Optin_Source_ID=s.Optin_Source_ID
		group by isnull(e.KeySourceSystem,s.KeySourceSystem)
			   , isnull(e.clientid,s.clientid)
			   , isnull(e.Optin_Source_ID,s.Optin_Source_ID)
			   , isnull(e.Optin_Month,s.Optin_Month)



END;