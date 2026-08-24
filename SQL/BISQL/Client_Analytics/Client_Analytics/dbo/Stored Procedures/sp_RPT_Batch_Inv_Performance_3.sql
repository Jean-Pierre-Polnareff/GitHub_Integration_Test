
CREATE PROCEDURE [dbo].[sp_RPT_Batch_Inv_Performance]
	 
	@StartDateTime DATETIME = NULL
	
AS
/* 
Object: dbo.sp_RPT_Batch_Inv_Performance

Description: Summarize account level activity for batches that are new or changed

Author			Date		Description 
Mike Campbell	05/25/2022	Created 
*/

BEGIN
	SET NOCOUNT ON;
	
	DECLARE @in_dt	DATE = CAST(CAST(DATEADD(DD, -1, ISNULL(@StartDateTime,GETDATE()))  AS DATE) AS DATETIME);

	DROP TABLE IF EXISTS #cur;

	SELECT DISTINCT list_mo
		    , keysourcesystem
			, clientid
	INTO #cur
	FROM (
				--NEW activity in DimCustomerSummary
				SELECT DISTINCT ISNULL(dd.MonthDate,dd2.monthdate) as list_mo
					   , dcs.keysourcesystem
					   , ISNULL(luc.client_id,dcu.clientid) as clientid
				FROM DW_MSTR_DM.dbo.dimcustomersummary dcs WITH (NOLOCK) 
						JOIN DW_MSTR_DM.dbo.dimsourcesystem dss WITH (NOLOCK) ON dcs.KeySourceSystem = dss.KeySourceSystem
						LEFT JOIN DW_MSTR_DM.dbo.LU_CUSTOMER luc WITH (NOLOCK) ON dcs.customerid = luc.customer_id AND dcs.keysourcesystem = 4
						LEFT JOIN dw_mstr_dm.dbo.dimcustomer dcu WITH (NOLOCK) ON dcs.keycustomer = dcu.keycustomer AND dss.sourcesystem2 = dcu.sourcesystem
						LEFT JOIN DW_MSTR_DM.dbo.dimdate dd WITH (NOLOCK) ON luc.list_date = dd.calendardate
						LEFT JOIN DW_MSTR_DM.dbo.dimdate dd2 WITH (NOLOCK) ON dcu.listdate = dd2.calendardate
				WHERE dcs.insertdate > @in_dt
					  OR dcs.updatedate > @in_dt
				--NEW non-FACS inventory
				UNION 
				SELECT DISTINCT
					   dd.MonthDate as list_mo
					   , dss.KeySourceSystem
					   , dcu.ClientId
				FROM DW_MSTR_DM.dbo.DimCustomer dcu WITH (NOLOCK) 
					JOIN DW_MSTR_DM.dbo.DimDate dd WITH (NOLOCK) ON dcu.ListDate=dd.CalendarDate
					JOIN DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK) ON dcu.SourceSystem=dss.SourceSystem2
				WHERE dcu.InsertDate > @in_dt
				--NEW FACS inventory
				UNION
				SELECT DISTINCT
					   dd.MonthDate as list_mo
					   , 4 as keysourcesystem
					   , luc.CLIENT_ID as clientid
				FROM DW_MSTR_DM.dbo.LU_CUSTOMER luc WITH (NOLOCK)
					JOIN DW_MSTR_DM.dbo.DimDate dd WITH (NOLOCK) ON luc.LIST_DATE=dd.CalendarDate
				WHERE luc.IMPORT_DATE > @in_dt
		     ) x;
	

	DROP TABLE IF EXISTS #TEMP_CUST

	SELECT  dd.monthdate 
		, dss.KeySourceSystem
		, dcu.clientid 
		, dcu.customerstate  
		, dcu.keycustomer  
		, GETDATE() as InsertDate 
		, dcu.CHARGEOFFDATE
		, dcu.LISTDATE
		, dcu.INITIALBALANCE 
	INTO #TEMP_CUST 
	FROM dw_mstr_dm.dbo.DimCustomer dcu WITH (NOLOCK)
		JOIN DW_MSTR_DM.dbo.DimDate dd WITH (NOLOCK) on dcu.ListDate=dd.CalendarDate
		JOIN DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK) on dcu.SourceSystem=dss.SourceSystem2
		JOIN #cur c on dd.MonthDate=c.list_mo
					AND dcu.ClientId=c.clientid
					AND dss.KeySourceSystem=c.KeySourceSystem

	CREATE INDEX #t_#TEMP_CUST on #TEMP_CUST (KeyCustomer,KeySourceSystem) INCLUDE (monthdate,clientid,customerstate,InsertDate,CHARGEOFFDATE,LISTDATE,INITIALBALANCE) 
	
	DROP TABLE IF EXISTS #TEMP_DCS
 
	SELECT t.monthdate 
		, t.KeySourceSystem
		, t.clientid 
		, t.customerstate  
		, t.keycustomer 
		, t.InsertDate 
		, t.CHARGEOFFDATE
		, t.LISTDATE
		, t.INITIALBALANCE 
		, dcs.calls
		, dcs.keycustomer dcs_keycustomer
		, dcs.customerid 
		, dcs.connected_calls 
		, dcs.rpc_calls 
		, dcs.calls_ib   
		, dcs.connected_calls_ib 
		, dcs.rpc_calls_ib 
		, dcs.calls_ob
		, dcs.connected_calls_ob 
		, dcs.rpc_calls_ob  
		, dcs.letters 
		, dcs.Payment_Ct  
		, dcs.Payment_Amt 
		, dcs.payment_amt_mo1_cum
		, dcs.payment_amt_mo2_cum
		, dcs.payment_amt_mo3_cum
		, dcs.payment_amt_mo4_cum
		, dcs.payment_amt_mo5_cum
		, dcs.payment_amt_mo6_cum
		, dcs.payment_amt_mo7_cum
		, dcs.payment_amt_mo8_cum
		, dcs.payment_amt_mo9_cum
		, dcs.payment_amt_mo10_cum
		, dcs.payment_amt_mo11_cum
		, dcs.payment_amt_mo12_cum
		, dcs.calls_mo1
		, dcs.calls_mo2
		, dcs.calls_mo3
		, dcs.payment_fee_amt
		, dcs.payment_ct_MTD
		, dcs.payment_amt_MTD 
		, dcs.payment_fee_amt_MTD
		, dcs.promise_calls_ob
		, dcs.promise_calls_ib 
	INTO #TEMP_DCS 
	FROM #TEMP_CUST t WITH (NOLOCK) 
		LEFT JOIN dw_mstr_dm.dbo.dimcustomersummary dcs WITH (NOLOCK) on t.KeyCustomer=dcs.KeyCustomer 
										AND t.KeySourceSystem=dcs.KeySourceSystem 
									                     
	DROP TABLE IF EXISTS #TEMP_CUST 

	--Delete from RPT_Batch_Inv_Performance where matching list_mo/keysourcesystem/clientid
	DELETE bip
	FROM CLIENT_ANALYTICS.dbo.RPT_Batch_Inv_Performance bip
		JOIN #cur c on bip.batch_month=c.list_mo
		           and bip.ClientID=c.clientid
				   and bip.KeySourceSystem=c.KeySourceSystem;

	--Reprocess summaries and insert to RPT_Batch_Inv_Performance for each list_mo/keysourcesystem/clientid

	--Non-FACS
	INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Batch_Inv_Performance
		(
			batch_month
			, KeySourceSystem
			, ClientID
			, CustomerState
			, accounts
			, balances
			, calls
			, unq_calls
			, connected_calls
			, unq_connected_calls
			, rpc_calls
			, unq_rpc_calls
			, calls_ib
			, unq_calls_ib
			, connected_calls_ib
			, unq_connected_calls_ib
			, rpc_calls_ib
			, unq_rpc_calls_ib
			, calls_ob
			, unq_calls_ob
			, connected_calls_ob
			, unq_connected_calls_ob
			, rpc_calls_ob
			, unq_rpc_calls_ob
			, letters
			, unq_letters
			, Payment_Ct
			, unq_Payers
			, Payment_Amt
			, InsertDate
			, payment_amt_mo1_cum
			, payment_amt_mo2_cum
			, payment_amt_mo3_cum
			, payment_amt_mo4_cum
			, payment_amt_mo5_cum
			, payment_amt_mo6_cum
			, payment_amt_mo7_cum
			, payment_amt_mo8_cum
			, payment_amt_mo9_cum
			, payment_amt_mo10_cum
			, payment_amt_mo11_cum
			, payment_amt_mo12_cum
			, calls_mo1
			, calls_mo2
			, calls_mo3
			, payment_fee_amt
			, payment_ct_MTD
			, payment_amt_MTD
			, payment_fee_amt_MTD
			, payment_ct_MTD_unq
			, promise_calls_ob
			, promise_calls_ib
			, Charge_Off_Ind 
			, Balance_Ranges
		) 
		SELECT t.monthdate as batch_month
			, t.KeySourceSystem
			, t.clientid as ClientID
			, t.customerstate as CustomerState
			, count(t.keycustomer) as accounts
			, sum(t.initialbalance) as balances
			, sum(t.calls) AS calls
			, count(DISTINCT case when t.calls>0 then isnull(dcs_keycustomer,t.customerid) end) AS unq_calls
			, sum(t.connected_calls) AS connected_calls
			, COUNT(DISTINCT CASE WHEN t.connected_calls>0 THEN isnull(t.dcs_keycustomer,t.customerid) END) AS unq_connected_calls
			, sum(t.rpc_calls) AS rpc_calls
			, COUNT(DISTINCT case when t.rpc_calls>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_rpc_calls
			, sum(t.calls_ib) AS calls_ib
			, count(DISTINCT case when t.calls_ib>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_calls_ib
			, sum(t.connected_calls_ib) AS connected_calls_ib
			, COUNT(DISTINCT CASE WHEN t.connected_calls_ib>0 THEN isnull(t.dcs_keycustomer,t.customerid) END) AS unq_connected_calls_ib
			, sum(t.rpc_calls_ib) AS rpc_calls_ib
			, COUNT(DISTINCT case when t.rpc_calls_ib>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_rpc_calls_ib
			, sum(t.calls_ob) AS calls_ob 
			, count(DISTINCT case when t.calls_ob>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_calls_ob
			, sum(t.connected_calls_ob) AS connected_calls_ob
			, COUNT(DISTINCT CASE WHEN t.connected_calls_ob>0 THEN isnull(t.dcs_keycustomer,t.customerid) END) AS unq_connected_calls_ob
			, sum(t.rpc_calls_ob) AS rpc_calls_ob
			, COUNT(DISTINCT case when t.rpc_calls_ob>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_rpc_calls_ob
			, sum(t.letters) as letters
			, COUNT(DISTINCT case when t .letters>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_letters
			, sum(t.Payment_Ct) as Payment_Ct
			, COUNT(DISTINCT case when t.payment_ct>0 then isnull(t.dcs_keycustomer,t.customerid) end) AS unq_Payers
			, sum(t.Payment_Amt) as Payment_Amt
			, GETDATE() as InsertDate
			, sum(t.payment_amt_mo1_cum) as payment_amt_mo1_cum
			, sum(t.payment_amt_mo2_cum) as payment_amt_mo2_cum
			, sum(t.payment_amt_mo3_cum) as payment_amt_mo3_cum
			, sum(t.payment_amt_mo4_cum) as payment_amt_mo4_cum
			, sum(t.payment_amt_mo5_cum) as payment_amt_mo5_cum
			, sum(t.payment_amt_mo6_cum) as payment_amt_mo6_cum
			, sum(t.payment_amt_mo7_cum) as payment_amt_mo7_cum
			, sum(t.payment_amt_mo8_cum) as payment_amt_mo8_cum
			, sum(t.payment_amt_mo9_cum) as payment_amt_mo9_cum
			, sum(t.payment_amt_mo10_cum) as payment_amt_mo10_cum
			, sum(t.payment_amt_mo11_cum) as payment_amt_mo11_cum
			, sum(t.payment_amt_mo12_cum) as payment_amt_mo12_cum
			, sum(t.calls_mo1) as calls_mo1
			, sum(t.calls_mo2) as calls_mo2
			, sum(t.calls_mo3) as calls_mo3
			, sum(t.payment_fee_amt) as payment_fee_amt
			, sum(t.payment_ct_MTD) as payment_ct_MTD
			, sum(t.payment_amt_MTD) as payment_amt_MTD
			, sum(t.payment_fee_amt_MTD) as payment_fee_amt_MTD
			, count(distinct case when t.payment_ct_MTD>0 
									then isnull(t.dcs_keycustomer,t.customerid) end) as payment_ct_MTD_unq
			, sum(t.promise_calls_ob) as promise_calls_ob
			, sum(t.promise_calls_ib) as promise_calls_ib
			, Charge_Off_Ind = CASE WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 0
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < .5
				THEN 'A - <6mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= .5
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 1
				THEN 'B -6mos-12mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 1
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 1.5
				THEN 'C -12mos-18mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 1.5
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 2
				THEN 'D -18mos-24mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 2
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 3
				THEN 'E -2yr-3yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 3
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 4
				THEN 'F -3yr-4yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 4
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 5
				THEN 'G -4yr-5yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 5
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 6
				THEN 'H -5yr-6yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 6
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 7
				THEN 'I -6yr-7yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 7
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 8
				THEN 'J -7yr-8yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 8
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 9
				THEN 'K -8yr-9yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 9
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 10
				THEN 'L -9yr-10yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 10
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 11
				THEN 'M -10yr-11yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 11
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 12
				THEN 'N -11yr-12yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 12
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 13
				THEN 'O -12yr-13yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 13
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 14
				THEN 'P -13yr-14yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 14
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 15
				THEN 'Q -14yr-15yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 15
				THEN 'R -15+ years'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 0
				THEN 'S -Missing Info'
			END
			, Balance_Ranges = CASE 
				WHEN t.INITIALBALANCE >0 AND t.INITIALBALANCE <500 THEN 'A-0-$499' 
				WHEN t.INITIALBALANCE >=500 AND t.INITIALBALANCE <1000 THEN 'B-$500-$999'
				WHEN t.INITIALBALANCE >=1000 AND t.INITIALBALANCE <2500 THEN 'C-$1000-$2499'
				WHEN t.INITIALBALANCE >=2500 AND t.INITIALBALANCE <4999 THEN 'D-$2500-$4999'
				WHEN t.INITIALBALANCE >=5000 THEN 'E-$5000+'
				END
		FROM #TEMP_DCS t 
		GROUP BY t.monthdate
					, t.KeySourceSystem
					, t.clientid
					, t.customerstate
					, CASE WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 0
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < .5
				THEN 'A - <6mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= .5
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 1
				THEN 'B -6mos-12mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 1
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 1.5
				THEN 'C -12mos-18mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 1.5
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 2
				THEN 'D -18mos-24mos'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 2
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 3
				THEN 'E -2yr-3yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 3
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 4
				THEN 'F -3yr-4yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 4
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 5
				THEN 'G -4yr-5yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 5
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 6
				THEN 'H -5yr-6yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 6
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 7
				THEN 'I -6yr-7yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 7
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 8
				THEN 'J -7yr-8yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 8
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 9
				THEN 'K -8yr-9yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 9
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 10
				THEN 'L -9yr-10yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 10
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 11
				THEN 'M -10yr-11yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 11
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 12
				THEN 'N -11yr-12yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 12
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 13
				THEN 'O -12yr-13yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 13
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 14
				THEN 'P -13yr-14yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 14
					AND DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 15
				THEN 'Q -14yr-15yrs'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 >= 15
				THEN 'R -15+ years'
				WHEN DATEDIFF(DAY,ISNULL(Cast(t.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(t.LISTDATE as date),'1/1/2050')) / 360.00 < 0
				THEN 'S -Missing Info'
			END
			, CASE 
				WHEN t.INITIALBALANCE >0 AND t.INITIALBALANCE <500 THEN 'A-0-$499'
				WHEN t.INITIALBALANCE >=500 AND t.INITIALBALANCE <1000 THEN 'B-$500-$999'
				WHEN t.INITIALBALANCE >=1000 AND t.INITIALBALANCE <2500 THEN 'C-$1000-$2499'
				WHEN t.INITIALBALANCE >=2500 AND t.INITIALBALANCE <4999 THEN 'D-$2500-$4999'
				WHEN t.INITIALBALANCE >=5000 THEN 'E-$5000+'
				END ; 

		DROP TABLE #TEMP_DCS; 

	--FACS
		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Batch_Inv_Performance
						   (
							  batch_month
							  , KeySourceSystem
							  , ClientID
							  , CustomerState
							  , accounts
							  , balances
							  , calls
							  , unq_calls
							  , connected_calls
							  , unq_connected_calls
							  , rpc_calls
							  , unq_rpc_calls
							  , calls_ib
							  , unq_calls_ib
							  , connected_calls_ib
							  , unq_connected_calls_ib
							  , rpc_calls_ib
							  , unq_rpc_calls_ib
							  , calls_ob
							  , unq_calls_ob
							  , connected_calls_ob
							  , unq_connected_calls_ob
							  , rpc_calls_ob
							  , unq_rpc_calls_ob
							  , letters
							  , unq_letters
							  , Payment_Ct
							  , unq_Payers
							  , Payment_Amt
							  , InsertDate
							  , payment_amt_mo1_cum
							  , payment_amt_mo2_cum
							  , payment_amt_mo3_cum
							  , payment_amt_mo4_cum
							  , payment_amt_mo5_cum
							  , payment_amt_mo6_cum
							  , payment_amt_mo7_cum
							  , payment_amt_mo8_cum
							  , payment_amt_mo9_cum
							  , payment_amt_mo10_cum
							  , payment_amt_mo11_cum
							  , payment_amt_mo12_cum
							  , calls_mo1
							  , calls_mo2
							  , calls_mo3
							  , payment_fee_amt
							  , payment_ct_MTD
							  , payment_amt_MTD
							  , payment_fee_amt_MTD
							  , payment_ct_MTD_unq
							  , promise_calls_ob
							  , promise_calls_ib
							  , Charge_Off_Ind
							  , Balance_Ranges
							)
			SELECT dd2.monthdate as batch_month
					, 4 as KeySourceSystem
					, luc.client_id as ClientID
					, luc.customer_state as CustomerState
					, count(luc.CUSTOMER_ID) as accounts
					, sum(obf.INITIAL_BALANCE) as balances
					, sum(dcs.calls) AS calls
					, count(DISTINCT case when dcs.calls>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_calls
					, sum(dcs.connected_calls) AS connected_calls
					, COUNT(DISTINCT CASE WHEN dcs.connected_calls>0 THEN isnull(dcs.keycustomer,dcs.customerid) END) AS unq_connected_calls
					, sum(dcs.rpc_calls) AS rpc_calls
					, COUNT(DISTINCT case when dcs.rpc_calls>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_rpc_calls
					, sum(dcs.calls_ib) AS calls_ib
					, count(DISTINCT case when dcs.calls_ib>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_calls_ib
					, sum(dcs.connected_calls_ib) AS connected_calls_ib
					, COUNT(DISTINCT CASE WHEN dcs.connected_calls_ib>0 THEN isnull(dcs.keycustomer,dcs.customerid) END) AS unq_connected_calls_ib
					, sum(dcs.rpc_calls_ib) AS rpc_calls_ib
					, COUNT(DISTINCT case when dcs.rpc_calls_ib>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_rpc_calls_ib
					, sum(dcs.calls_ob) AS calls_ob
					, count(DISTINCT case when dcs.calls_ob>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_calls_ob
					, sum(dcs.connected_calls_ob) AS connected_calls_ob
					, COUNT(DISTINCT CASE WHEN dcs.connected_calls_ob>0 THEN isnull(dcs.keycustomer,dcs.customerid) END) AS unq_connected_calls_ob
					, sum(dcs.rpc_calls_ob) AS rpc_calls_ob
					, COUNT(DISTINCT case when dcs.rpc_calls_ob>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_rpc_calls_ob
					, sum(dcs.letters) as letters
					, COUNT(DISTINCT case when dcs.letters>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_letters
					, sum(dcs.Payment_Ct) as Payment_Ct
					, COUNT(DISTINCT case when dcs.payment_ct>0 then isnull(dcs.keycustomer,dcs.customerid) end) AS unq_Payers
					, sum(dcs.Payment_Amt) as Payment_Amt
					, GETDATE() as InsertDate
				    , sum(dcs.payment_amt_mo1_cum) as payment_amt_mo1_cum
				    , sum(dcs.payment_amt_mo2_cum) as payment_amt_mo2_cum
				    , sum(dcs.payment_amt_mo3_cum) as payment_amt_mo3_cum
				    , sum(dcs.payment_amt_mo4_cum) as payment_amt_mo4_cum
				    , sum(dcs.payment_amt_mo5_cum) as payment_amt_mo5_cum
				    , sum(dcs.payment_amt_mo6_cum) as payment_amt_mo6_cum
				    , sum(dcs.payment_amt_mo7_cum) as payment_amt_mo7_cum
				    , sum(dcs.payment_amt_mo8_cum) as payment_amt_mo8_cum
				    , sum(dcs.payment_amt_mo9_cum) as payment_amt_mo9_cum
				    , sum(dcs.payment_amt_mo10_cum) as payment_amt_mo10_cum
				    , sum(dcs.payment_amt_mo11_cum) as payment_amt_mo11_cum
				    , sum(dcs.payment_amt_mo12_cum) as payment_amt_mo12_cum
					, sum(dcs.calls_mo1) as calls_mo1
					, sum(dcs.calls_mo2) as calls_mo2
					, sum(dcs.calls_mo3) as calls_mo3
					, sum(dcs.payment_fee_amt) as payment_fee_amt
					, sum(dcs.payment_ct_MTD) as payment_ct_MTD
					, sum(dcs.payment_amt_MTD) as payment_amt_MTD
					, sum(dcs.payment_fee_amt_MTD) as payment_fee_amt_MTD
					, count(distinct case when dcs.payment_ct_MTD>0 
					                      then isnull(dcs.keycustomer,dcs.customerid) end) as payment_ct_MTD_unq
					, sum(dcs.promise_calls_ob) as promise_calls_ob
					, sum(dcs.promise_calls_ib) as promise_calls_ib
					, Charge_Off_Ind = CASE WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 0
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < .5
             THEN 'A - <6mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= .5
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 1
             THEN 'B -6mos-12mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 1
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 1.5
             THEN 'C -12mos-18mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 1.5
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 2
             THEN 'D -18mos-24mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 2
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 3
             THEN 'E -2yr-3yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 3
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 4
             THEN 'F -3yr-4yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 4
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 5
             THEN 'G -4yr-5yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 5
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 6
             THEN 'H -5yr-6yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 6
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 7
             THEN 'I -6yr-7yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 7
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 8
             THEN 'J -7yr-8yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 8
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 9
             THEN 'K -8yr-9yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 9
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 10
             THEN 'L -9yr-10yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 10
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 11
             THEN 'M -10yr-11yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 11
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 12
             THEN 'N -11yr-12yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 12
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 13
             THEN 'O -12yr-13yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 13
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 14
             THEN 'P -13yr-14yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 14
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 15
             THEN 'Q -14yr-15yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 15
             THEN 'R -15+ years'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 0
             THEN 'S -Missing Info'
        END
		, Balance_Ranges = CASE 
				WHEN obf.INITIAL_BALANCE >0 AND obf.INITIAL_BALANCE <500 THEN 'A-0-$499'
				WHEN obf.INITIAL_BALANCE >=500 AND obf.INITIAL_BALANCE <1000 THEN 'B-$500-$999'
				WHEN obf.INITIAL_BALANCE >=1000 AND obf.INITIAL_BALANCE <2500 THEN 'C-$1000-$2499'
				WHEN obf.INITIAL_BALANCE >=2500 AND obf.INITIAL_BALANCE <4999 THEN 'D-$2500-$4999'
				WHEN obf.INITIAL_BALANCE >=5000 THEN 'E-$5000+'
				END

			from DW_MSTR_DM.dbo.LU_CUSTOMER luc (nolock)
				    join
				 DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT obf (nolock) on luc.CUSTOMER_ID=obf.CUSTOMER_ID
					join
				 DW_MSTR_DM.dbo.DimDate dd2 (nolock) on luc.list_date=dd2.CalendarDate				
				    join
				 #cur c on dd2.MonthDate=c.list_mo
				           and luc.CLIENT_ID=c.clientid
						   and c.KeySourceSystem=4
					left join
				 dw_mstr_dm.dbo.dimcustomersummary dcs (nolock) on dcs.CustomerID = luc.customer_id 
																and dcs.clientid = luc.CLIENT_ID 
																and dcs.KeySourceSystem = 4
			GROUP BY dd2.monthdate
					, luc.client_id
					, luc.customer_state
					, CASE WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 0
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < .5
             THEN 'A - <6mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= .5
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 1
             THEN 'B -6mos-12mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 1
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 1.5
             THEN 'C -12mos-18mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 1.5
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 2
             THEN 'D -18mos-24mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 2
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 3
             THEN 'E -2yr-3yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 3
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 4
             THEN 'F -3yr-4yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 4
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 5
             THEN 'G -4yr-5yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 5
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 6
             THEN 'H -5yr-6yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 6
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 7
             THEN 'I -6yr-7yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 7
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 8
             THEN 'J -7yr-8yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 8
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 9
             THEN 'K -8yr-9yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 9
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 10
             THEN 'L -9yr-10yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 10
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 11
             THEN 'M -10yr-11yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 11
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 12
             THEN 'N -11yr-12yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 12
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 13
             THEN 'O -12yr-13yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 13
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 14
             THEN 'P -13yr-14yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 14
                  AND DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 15
             THEN 'Q -14yr-15yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 >= 15
             THEN 'R -15+ years'
             WHEN DATEDIFF(DAY,ISNULL(Cast(luc.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(luc.LIST_DATE as date),'1/1/2050')) / 360.00 < 0
             THEN 'S -Missing Info'
        END
		, CASE 
				WHEN obf.INITIAL_BALANCE >0 AND obf.INITIAL_BALANCE <500 THEN 'A-0-$499'
				WHEN obf.INITIAL_BALANCE >=500 AND obf.INITIAL_BALANCE <1000 THEN 'B-$500-$999'
				WHEN obf.INITIAL_BALANCE >=1000 AND obf.INITIAL_BALANCE <2500 THEN 'C-$1000-$2499'
				WHEN obf.INITIAL_BALANCE >=2500 AND obf.INITIAL_BALANCE <4999 THEN 'D-$2500-$4999'
				WHEN obf.INITIAL_BALANCE >=5000 THEN 'E-$5000+'
				END;


END;