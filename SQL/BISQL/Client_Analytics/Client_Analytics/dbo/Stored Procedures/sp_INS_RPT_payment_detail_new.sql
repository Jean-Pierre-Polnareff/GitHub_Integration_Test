


CREATE PROCEDURE [dbo].[sp_INS_RPT_payment_detail_new]
-- =============================================
--	Object: dbo.sp_INS_RPT_payment_detail_new
--
--  Description: insert new payment details to dbo.RPT_payment_detail

-- 	History
-- 	Author		Date		Description
-- 	------------------------------------------------------
-- 	mcampbell	3/17/20		Created 
-- =======================================================================
AS

SET NOCOUNT ON;

--3/26/21 - removing @maxdt as it was causing payments to be missed. Changing to 7 days back cleanse and repopulate.
--	DECLARE @maxdt date = (SELECT MAX(pymt_date) FROM CLIENT_ANALYTICS.dbo.RPT_payment_detail);

	declare @pymt_date date = DATEADD(DAY,-7,CAST(GETDATE() AS DATE))

    DELETE FROM CLIENT_ANALYTICS.dbo.RPT_payment_detail
	  WHERE pymt_date >= @pymt_date;

	--FACS portion of union qry
	SELECT 'FACS' AS crm
			, pay.CUSTOMER_ID
			, Cust.CUSTOMER_STATE
			, pay.pymt_date
			, st.parent AS client_parent
			, ST.Client_Stream
			, ST.PaperType
			, ST.Vertical
			, ST.PaperSource
			, CASE when st.location_worked='New Jersey' then 'Thorofare'
					when st.location_worked='NJ' then 'Ramsey'
					else st.Location_Worked
					END AS Location_Worked
			, CL.CLIENT_DESC
			, PAY.CLIENT_ID
			, PAY.CREDITED_EMPLOYEE_ID
			, E.DEPARTMENT_ID
			, E.JOB_CODE
			, e.DESK
			, PAY.PYMT_TYPE
			, CASE WHEN PAY.PYMT_TYPE IN ('MC','VI','CC') THEN 'Credit_Card'
					WHEN PAY.PYMT_TYPE IN ('DC') THEN 'Debit_Card'
					WHEN PAY.PYMT_TYPE IN ('DP') THEN 'Direct_Pay'
					WHEN PAY.PYMT_TYPE IN ('EFT') THEN 'Elect_Fund_Tran'
					WHEN PAY.PYMT_TYPE IN ('CK','CKP') THEN 'Check'
					ELSE 'OTHER' END
					AS PYMT_TYPE_DSCR
		, CASE WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) BETWEEN 0 AND 6 THEN 'A - <6mos'
				WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) BETWEEN 7 AND 12 THEN 'B - 7-12mos'
				WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) BETWEEN 13 AND 23 THEN 'C - 1-2yr'
				WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) BETWEEN 24 AND 35 THEN 'D - 2-3yr'
				WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) BETWEEN 36 AND 59 THEN 'E - 3-5yr'
				WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) BETWEEN 60 AND 119 THEN 'F - 6-10yr'
				WHEN DATEDIFF(MONTH,cust.charge_off_date,cust.list_date) >=120 THEN 'G - 10+yr'
				WHEN cust.charge_off_date IS NULL OR cust.list_date IS NULL OR cust.list_date<cust.charge_off_date THEN 'H - Missing'
				END AS ChargeOffAge
		, CASE WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) BETWEEN 0 AND 6 THEN 'A - <6mos'
				WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) BETWEEN 7 AND 12 THEN 'B - 7-12mos'
				WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) BETWEEN 13 AND 23 THEN 'C - 1-2yr'
				WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) BETWEEN 24 AND 35 THEN 'D - 2-3yr'
				WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) BETWEEN 36 AND 59 THEN 'E - 3-5yr'
				WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) BETWEEN 60 AND 119 THEN 'F - 6-10yr'
				WHEN DATEDIFF(MONTH,cust.list_date,pay.pymt_date) >=120 THEN 'G - 10+yr'
				WHEN cust.list_date IS NULL OR pay.pymt_date<cust.list_date THEN 'H - Missing'
				END AS ListAgeAtPymt           
		, cust.CHARGE_OFF_DATE
		, cust.LIST_DATE
		, CASE WHEN fp.PAYMENT_FACT_ID IS NOT NULL THEN 1 ELSE 0 END AS first_payment_flag
		, pay.PAYMENT_AMT_APPLIED AS total_collections
		, pay.AMT_DUE_AGENCY AS total_fees
		, usb.ACCOUNT_NUM AS usb_account_num
		, usb.Client_ID AS usb_client_id
		, usb.CUST_TYPE AS usb_cust_type
		, usb.PORTFOLIO_ID AS usb_pprtfolio_id
		, usb.owner_id AS usb_owner_id
		, CASE WHEN pay.PAYMENT_AMT_APPLIED > 0 THEN 1 ELSE 0 end AS total_payments


	INTO #PD
	FROM    DW_MSTR_DM.dbo.PAYMENT_FACT PAY (NOLOCK)
	LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_CUSTOMER Cust (NOLOCK)
		ON PAY.CUSTOMER_ID = Cust.CUSTOMER_ID
	LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT (NOLOCK) 
		ON Cust.LIST_DATE = DT.CALNDR_DT
	LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT2 (NOLOCK)
		ON PAY.PYMT_DATE = DT2.CALNDR_DT
	LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_CLIENT CL (NOLOCK)
		ON PAY.CLIENT_ID = CL.CLIENT_ID
	LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE E (NOLOCK) 
		ON PAY.CREDITED_EMPLOYEE_ID = E.EMPLOYEE_ID
	LEFT OUTER JOIN DW_MSTR_DM.dbo.TblClientStreams ST (NOLOCK) 
		ON PAY.CLIENT_ID = ST.Client_ID
	LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT3 (NOLOCK)
		ON Cust.CHARGE_OFF_DATE = DT3.CALNDR_DT
	LEFT JOIN DW_MSTR_DM.dbo.USBankRetail_Codes usb (NOLOCK) --7/21/16          
				ON PAY.CUSTOMER_ID = usb.ACCOUNT_NUM
	LEFT JOIN DW_MSTR_DM.dbo.FIRST_PAYMENT fp (NOLOCK) --8/29/16
		ON pay.PAYMENT_FACT_ID = fp.PAYMENT_FACT_ID
	--LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT4  --8/29/16
	--	ON fp.FirstPaymentDate = DT4.CALNDR_DT	
	LEFT OUTER JOIN dw_mstr_dm.dbo.DeskLocation	DK (NOLOCK) 
		ON PAY.CREDITED_EMPLOYEE_ID	= DK.Desk_ID
	LEFT OUTER JOIN dw_mstr_dm.dbo.TblDeptLocation	DL (NOLOCK) 
		ON E.DEPARTMENT_ID	= DL.Dept_Id		
	WHERE  PAY.PYMT_TYPE NOT IN ('DBJ','CRJ','PCK','CAN')
			AND pay.PYMT_DATE >= @pymt_date

	UNION all
    
	--non-FACS        
	SELECT dss.SourceSystem AS crm
			, dcu.CustomerId
			, dcu.CustomerState
			, dd.CalendarDate AS pymt_dt
			, dcl.ClientParentGroup AS client_parent					
			, dcl.ClientStream AS clientstream
			, dcl.PaperType
			, dcl.ClientClass AS vertical
			, NULL AS papersource
			, CASE when dcl.LocationWorked='New Jersey' then 'Thorofare'
					when dcl.LocationWorked='NJ' then 'Ramsey'
					else dcl.LocationWorked
					END AS location_worked
			, NULL AS client_desc
			, dcl.ClientId
			, de.EmployeeId
			, de.DepartmentId
			, de.JobCode
			, de.Desk
			, dpt.PaymentType
			, dpt.PaymentTypeDescription AS pymt_type_dscr
			, CASE WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) BETWEEN 0 AND 6 THEN 'A - <6mos'
					WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) BETWEEN 7 AND 12 THEN 'B - 7-12mos'
					WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) BETWEEN 13 AND 23 THEN 'C - 1-2yr'
					WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) BETWEEN 24 AND 35 THEN 'D - 2-3yr'
					WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) BETWEEN 36 AND 59 THEN 'E - 3-5yr'
					WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) BETWEEN 60 AND 119 THEN 'F - 6-10yr'
					WHEN DATEDIFF(MONTH,dcu.chargeoffdate,dcu.listdate) >=120 THEN 'G - 10+yr'
					WHEN dcu.chargeoffdate IS NULL OR dcu.listdate IS NULL OR dcu.listdate<dcu.chargeoffdate THEN 'H - Missing'							
					END AS ChargeOffAge
			, CASE WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) BETWEEN 0 AND 6 THEN 'A - <6mos'
					WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) BETWEEN 7 AND 12 THEN 'B - 7-12mos'
					WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) BETWEEN 13 AND 23 THEN 'C - 1-2yr'
					WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) BETWEEN 24 AND 35 THEN 'D - 2-3yr'
					WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) BETWEEN 36 AND 59 THEN 'E - 3-5yr'
					WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) BETWEEN 60 AND 119 THEN 'F - 6-10yr'
					WHEN DATEDIFF(MONTH,dcu.listdate,dd.calendardate) >=120 THEN 'G - 10+yr'
					WHEN dcu.listdate IS NULL OR dd.calendardate<dcu.listdate THEN 'H - Missing'
					END AS ListAgeAtPymt
			, dcu.ChargeOffDate
			, dcu.ListDate
			, ISNULL(fcp.IsFirstPayment,0) AS first_payment_flag
--1/12/21 - COMMENTING OUT THESE FOR AMEX AFTER FACTCUSTOMERPAYMENT FIX
			--, CASE WHEN dss.sourcesystem LIKE 'Amex%' AND dpt.PaymentType LIKE '%NSF' THEN -fcp.PaymentAppliedAmt 
			--	   ELSE fcp.PaymentAppliedAmt 
			--	   END AS total_collections
			--, CASE WHEN dss.sourcesystem LIKE 'Amex%' AND dpt.PaymentType LIKE '%NSF' THEN -fcp.AgencyDueAmt 
			--	   ELSE fcp.AgencyDueAmt 
			--	   END AS total_fees
--1/12/21  - UNCOMMENTING THESE AFTER FACTCUSTOMERPAYMENT FIX
            , fcp.PaymentAppliedAmt AS total_collections
			, fcp.AgencyDueAmt AS total_fees
			, NULL AS usb_account_num
			, NULL AS usb_client_id
			, NULL AS usb_cust_type
			, NULL asusb_portfolio_id
			, NULL AS usb_owner_id
		    , CASE WHEN fcp.PaymentAppliedAmt > 0 THEN 1 ELSE 0 end AS total_payments

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
	WHERE dd.CalendarDate >= @pymt_date 
			AND (dpt.PaymentCategory <> 'Adjustment' OR dpt.PaymentCategory IS NULL)
			AND dpt.PaymentType NOT IN('DA','DAR')


	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_payment_detail
	(
	    crm,
	    CUSTOMER_ID,
	    CUSTOMER_STATE,
	    pymt_date,
	    client_parent,
	    Client_Stream,
	    PaperType,
	    Vertical,
	    PaperSource,
	    Location_Worked,
	    CLIENT_DESC,
	    CLIENT_ID,
	    CREDITED_EMPLOYEE_ID,
	    DEPARTMENT_ID,
	    JOB_CODE,
	    DESK,
	    PYMT_TYPE,
	    PYMT_TYPE_DSCR,
	    ChargeOffAge,
	    ListAgeAtPymt,
	    CHARGE_OFF_DATE,
	    LIST_DATE,
	    first_payment_flag,
	    total_collections,
	    total_fees,
	    usb_account_num,
	    usb_client_id,
	    usb_cust_type,
	    usb_pprtfolio_id,
	    usb_owner_id,
		total_payments
	)
	SELECT * FROM #PD
GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORPGLBDOM\ranthony]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORPGLBDOM\ranthony]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\musalunke]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\musalunke]
    AS [dbo];




GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\ranthony]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\ranthony]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_INS_RPT_payment_detail_new] TO [CORP\aughodake]
    AS [dbo];

