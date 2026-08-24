






CREATE PROCEDURE [dbo].[sp_insert_kpi_rpt]

AS
/* 
Object: dbo.insert_rpt_cv19

Description: Identify and insert dialer exceptions into fact_dial_excpt

Author			Date		Description
Mike Campbell	04/16/2020	Created
Mike Campbell	12/09/2020	Modified payments portion to properly remove adjustments and negate NSFs for Amex
Mike Campbell	01/14/2021	NSFs/corrections properly negated in FCP.  Modified payments for non-FACS to not negate.
Mike Campbell	02/18/2021	Moved from MN5 to BISQL.
*/

BEGIN
	SET NOCOUNT ON;

	declare @after_c date=(select MAX(calendardate) from CLIENT_ANALYTICS.dbo.kpi_rpt_calls);

	--calls
	insert into CLIENT_ANALYTICS.dbo.kpi_rpt_calls
	             (crm
				  ,CalendarDate
				  ,MonthDate
				  ,client
				  ,Location_Worked
				  ,call_direction
				  ,calls
				  ,rpc_calls
				  ,promise_calls
				  ,connect_calls)
	SELECT 'FACS' AS crm
		   , dd.CalendarDate
		   , dd.MonthDate
		   , st.parent as client
		   , st.Location_Worked
		   , case when PAY.CALL_TYPE<>'IN' then 'Outbound'
				  else 'Inbound'
				  end as call_direction
		, COUNT(pay.customer_id) as calls
		, COUNT(case when PAY.IsAdjRPC=1 then PAY.CUSTOMER_ID end) as rpc_calls
		, COUNT(case when PAY.IsPromise=1 then PAY.CUSTOMER_ID end) as promise_calls
		, COUNT(case when pay.CONTACT_CODE<>'XNA' and len(contact_code)>0 then pay.CALL_HISTORY_FACT_ID end) as connect_calls
	FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT PAY (NOLOCK)
			 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.LU_CUSTOMER Cust (NOLOCK) ON PAY.CUSTOMER_ID = Cust.CUSTOMER_ID
			 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.LU_CLIENT CL (NOLOCK) ON PAY.CLIENT_ID = CL.CLIENT_ID
			 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.LU_EMPLOYEE E (NOLOCK) ON PAY.EMPLOYEE_ID = E.EMPLOYEE_ID
			 LEFT OUTER JOIN 
		 DW_MSTR_DM.dbo.TblClientStreams ST (NOLOCK) ON PAY.CLIENT_ID = ST.Client_ID
			 join 
		 DW_MSTR_DM.dbo.DimDate dd (NOLOCK) on PAY.CALL_DATE=dd.CalendarDate
	WHERE  pay.call_DATE > @after_c 
		   and PAY.CALL_DATE < CAST(getdate() as DATE)
	group by dd.CalendarDate
		   , dd.MonthDate
		   , st.parent
		   , ST.Location_Worked
		   , case when PAY.CALL_TYPE<>'IN' then 'Outbound'
				  else 'Inbound'
				  end
	UNION
	SELECT dss.SourceSystem AS crm
		   , dd.CalendarDate
		   , dd.MonthDate
			,dcl.ClientParentGroup  AS client
			, case when dss.sourcesystem like 'Amex%' then 'India'
				   else dcl.LocationWorked
				   end as locationworked
			, case when fcp.IsInbound=1 then 'Inbound'
				   else 'Outbound'
				   end as call_direction
			, count(dcu.CustomerId) as calls
			, COUNT(case when fcp.IsRPC=1 then dcu.CustomerId end) as rpc_calls
			, COUNT(case when fcp.IsPromise=1 then dcu.CustomerId end) as promise_calls
			, COUNT(case when fcp.IsConnect=1 then dcu.CUSTOMERID end) as connect_calls
	FROM DW_MSTR_DM.dbo.FactCustomerCall fcp (NOLOCK)
			   LEFT JOIN
			DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcp.KeyCustomer=dcu.KeyCustomer
			   LEFT JOIN
			DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) ON fcp.KeyEmployee=de.KeyEmployee
			   LEFT JOIN
			DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON fcp.KeyClient=dcl.KeyClient
			   LEFT JOIN
			DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON fcp.KeyDate_CallDate=dd.KeyDate
			   INNER JOIN
			DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK) ON fcp.KeySourceSystem=dss.KeySourceSystem
	WHERE dd.CalendarDate > @after_c 
		  and dd.CalendarDate < CAST(getdate() as DATE)
	group by dss.SourceSystem
		   , dd.CalendarDate
		   , dd.MonthDate
			, dcl.ClientParentGroup 
			, case when dss.sourcesystem like 'Amex%' then 'India'
				   else dcl.LocationWorked
				   end
			, case when fcp.IsInbound=1 then 'Inbound'
				   else 'Outbound'
				   end
	option (recompile)


	--payments
		truncate table CLIENT_ANALYTICS.dbo.kpi_rpt_payments
	    
		insert into CLIENT_ANALYTICS.dbo.kpi_rpt_payments
		             (crm
					  ,CalendarDate
					  ,MonthDate
					  ,client
					  ,Location_Worked
					  ,total_payers
					  ,total_collections
					  ,total_fees)
		SELECT 'FACS' AS crm
			   , dd.CalendarDate
			   , dd.MonthDate
			   , st.parent as client
			   , ST.Location_Worked
			, COUNT(distinct pay.customer_id) as total_payers
			, sum(pay.PAYMENT_AMT_APPLIED) AS total_collections
			, sum(pay.AMT_DUE_AGENCY) AS total_fees
		FROM DW_MSTR_DM.dbo.PAYMENT_FACT PAY (NOLOCK)
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.LU_CUSTOMER Cust (NOLOCK) ON PAY.CUSTOMER_ID = Cust.CUSTOMER_ID
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.LU_DATE DT ON Cust.LIST_DATE = DT.CALNDR_DT
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.LU_DATE DT2 ON PAY.PYMT_DATE = DT2.CALNDR_DT
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.LU_CLIENT CL ON PAY.CLIENT_ID = CL.CLIENT_ID
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.LU_EMPLOYEE E ON PAY.CREDITED_EMPLOYEE_ID = E.EMPLOYEE_ID
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.TblClientStreams ST ON PAY.CLIENT_ID = ST.Client_ID
				LEFT OUTER JOIN 
			 DW_MSTR_DM.dbo.LU_DATE DT3 ON Cust.CHARGE_OFF_DATE = DT3.CALNDR_DT
				LEFT JOIN 
			 DW_MSTR_DM.dbo.USBankRetail_Codes usb ON PAY.CUSTOMER_ID = usb.ACCOUNT_NUM
				LEFT JOIN 
			 DW_MSTR_DM.dbo.FIRST_PAYMENT fp ON pay.PAYMENT_FACT_ID = fp.PAYMENT_FACT_ID
				LEFT OUTER JOIN 
			 dw_mstr_dm.dbo.DeskLocation DK ON PAY.CREDITED_EMPLOYEE_ID	= DK.Desk_ID
				LEFT OUTER JOIN 
			 dw_mstr_dm.dbo.TblDeptLocation	DL ON E.DEPARTMENT_ID	= DL.Dept_Id		
				join 
			 DW_MSTR_DM.dbo.DimDate dd on PAY.PYMT_DATE=dd.CalendarDate
		WHERE  PAY.PYMT_TYPE NOT IN ('DBJ','CRJ','PCK','CAN')
			   AND pay.PYMT_DATE > '12/31/19'
			   and pay.PYMT_DATE < CAST(getdate() as DATE)
		group by dd.CalendarDate
			   , dd.MonthDate
			   , st.parent
			   , ST.Location_Worked
		UNION
		SELECT dss.SourceSystem AS crm
			   , dd.CalendarDate
			   , dd.MonthDate
				, dcl.ClientParentGroup  AS client
				, case when dss.sourcesystem like 'Amex%' then 'India'
					   else dcl.LocationWorked
					   end as location_worked
				, count(distinct dcu.CustomerId) as total_payers
--1/14/21 FCP corrections properly negated so removing NSF logic for Amex.  commenting out
				--, sum(CASE WHEN dss.SourceSystem like 'Amex%' 
				--                and dpt.PaymentType LIKE '%NSF' 
				--               THEN -fcp.PaymentAppliedAmt 
				--           ELSE fcp.PaymentAppliedAmt end) AS total_collections
				--, sum(CASE WHEN dss.SourceSystem like 'Amex%' 
				--                and dpt.PaymentType LIKE '%NSF' 
				--               THEN -fcp.AgencyDueAmt 
				--           else fcp.AgencyDueAmt end) AS total_fees
				, sum(fcp.PaymentAppliedAmt) AS total_collections
				, sum(fcp.AgencyDueAmt) AS total_fees
		FROM DW_MSTR_DM.dbo.FactCustomerPayment fcp (NOLOCK)
				   LEFT JOIN
				DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcp.KeyCustomer=dcu.KeyCustomer
				   LEFT JOIN
				DW_MSTR_DM.dbo.DimEmployee de (NOLOCK) ON fcp.KeyEmployee=de.KeyEmployee
				   LEFT JOIN
				DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON fcp.KeyClient=dcl.KeyClient
				   LEFT JOIN
				DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON fcp.KeyDate_PaymentDate=dd.KeyDate
				   LEFT JOIN
				DW_MSTR_DM.dbo.DimPaymentTransactionStatus dpts (NOLOCK) ON fcp.KeyPaymentTransactionStatus=dpts.KeyPaymentTransactionStatus
				   LEFT JOIN
				DW_MSTR_DM.dbo.DimPaymentType dpt (NOLOCK) ON fcp.KeyPaymentType=dpt.KeyPaymentType
				   INNER JOIN
				DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK) ON fcp.KeySourceSystem=dss.KeySourceSystem
		WHERE dd.CalendarDate > '12/31/19'
			  and dd.CalendarDate < CAST(GETDATE()  AS DATE)
			  AND (dpt.PaymentCategory<>'Adjustment' OR dpt.PaymentCategory IS NULL)
			  and dpt.PaymentType not in('DA','DAR')
			  and dpt.PaymentTypeDescription<>'Adjustment'
		group by dss.SourceSystem
			   , dd.CalendarDate
			   , dd.MonthDate
				,dcl.ClientParentGroup 
				, case when dss.sourcesystem like 'Amex%' then 'India'
					   else dcl.LocationWorked
					   end;

		delete from [client_analytics].[dbo].[RPT_KPI_calls];

		insert into [client_analytics].[dbo].[RPT_KPI_calls]
		select *
		from CLIENT_ANALYTICS.dbo.vw_rpt_kpi_calls;


		delete from [client_analytics].[dbo].[RPT_KPI_payments];

		insert into [client_analytics].[dbo].[RPT_KPI_payments]
		select *
		from CLIENT_ANALYTICS.dbo.vw_rpt_kpi_payments;


END;

GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_insert_kpi_rpt] TO [CORP\aramugade]
    AS [dbo];

