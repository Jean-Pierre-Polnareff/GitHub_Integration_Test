USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_RPT_Batch_Inv_Performance]    Script Date: 5/25/2022 6:26:25 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO






alter PROCEDURE [dbo].[sp_RPT_Batch_Inv_Performance]
	
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

	declare 
		@in_dt	date=(select max(cast(insertdate as date)) 
		                from CLIENT_ANALYTICS.dbo.RPT_Batch_Inv_Performance);

		drop table if exists #cur;

		select distinct list_mo
		       , keysourcesystem
			   , clientid
		into #cur
		from (
				--NEW activity in DimCustomerSummary
				select distinct isnull(dd.MonthDate,dd2.monthdate) as list_mo
					   , dcs.keysourcesystem
					   , isnull(luc.client_id,dcu.clientid) as clientid
				from DW_MSTR_DM.dbo.dimcustomersummary dcs (nolock)
						join
					 DW_MSTR_DM.dbo.dimsourcesystem dss (nolock) on dcs.KeySourceSystem=dss.KeySourceSystem
						left join
					 DW_MSTR_DM.dbo.LU_CUSTOMER luc (nolock) on dcs.customerid=luc.customer_id and dcs.keysourcesystem=4
						left join
					 dw_mstr_dm.dbo.dimcustomer dcu (nolock) on dcs.keycustomer=dcu.keycustomer and dss.sourcesystem2=dcu.sourcesystem
						left join
					 DW_MSTR_DM.dbo.dimdate dd (nolock) on luc.list_date=dd.calendardate
						left join
					 DW_MSTR_DM.dbo.dimdate dd2 (nolock) on dcu.listdate=dd2.calendardate
				where dcs.insertdate>@in_dt
					  or dcs.updatedate>@in_dt
				--NEW non-FACS inventory
				union
				select distinct
					   dd.MonthDate as list_mo
					   , dss.KeySourceSystem
					   , dcu.ClientId
				from DW_MSTR_DM.dbo.DimCustomer dcu (nolock)
					   join
					 DW_MSTR_DM.dbo.DimDate dd (nolock) on dcu.ListDate=dd.CalendarDate
					   join
					 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on dcu.SourceSystem=dss.SourceSystem2
				where dcu.InsertDate>@in_dt
				--NEW FACS inventory
				union
				select distinct
					   dd.MonthDate as list_mo
					   , 4 as keysourcesystem
					   , luc.CLIENT_ID as clientid
				from DW_MSTR_DM.dbo.LU_CUSTOMER luc (nolock)
					   join
					 DW_MSTR_DM.dbo.DimDate dd (nolock) on luc.LIST_DATE=dd.CalendarDate
				where luc.IMPORT_DATE>@in_dt
		     ) x;

	--Delete from RPT_Batch_Inv_Performance where matching list_mo/keysourcesystem/clientid
	DELETE bip
	FROM CLIENT_ANALYTICS.dbo.RPT_Batch_Inv_Performance bip
	       join
		 #cur c on bip.batch_month=c.list_mo
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
							)
			SELECT dd.monthdate as batch_month
					, dss.KeySourceSystem
					, dcu.clientid as ClientID
					, dcu.customerstate as CustomerState
					, count(dcu.keycustomer) as accounts
					, sum(dcu.initialbalance) as balances
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
			from dw_mstr_dm.dbo.DimCustomer dcu (nolock)
					join
				 DW_MSTR_DM.dbo.DimDate dd (nolock) on dcu.ListDate=dd.CalendarDate
					join
				 DW_MSTR_DM.dbo.DimSourceSystem dss (nolock) on dcu.SourceSystem=dss.SourceSystem2
				    join
				 #cur c on dd.MonthDate=c.list_mo
				           and dcu.ClientId=c.clientid
						   and dss.KeySourceSystem=c.KeySourceSystem
					left join
				 dw_mstr_dm.dbo.dimcustomersummary dcs (nolock) on dcu.KeyCustomer=dcs.KeyCustomer and dss.KeySourceSystem=dcs.KeySourceSystem
			GROUP BY dd.monthdate
					, dss.KeySourceSystem
					, dcu.clientid
					, dcu.customerstate;

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
				 dw_mstr_dm.dbo.dimcustomersummary dcs (nolock) on luc.customer_id=dcs.CustomerID and dcs.KeySourceSystem=4
			GROUP BY dd2.monthdate
					, luc.client_id
					, luc.customer_state;


END;
GO


