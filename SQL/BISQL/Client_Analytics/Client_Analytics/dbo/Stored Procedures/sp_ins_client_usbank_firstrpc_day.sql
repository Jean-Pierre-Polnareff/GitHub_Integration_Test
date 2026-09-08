






CREATE PROCEDURE [dbo].[sp_ins_client_usbank_firstrpc_day]

AS
/* 
Object: dbo.sp_ins_client_usbank_firstrpc_day

Author			Date		Description
Mike Campbell	05/13/2020	Created

*/

BEGIN
	SET NOCOUNT ON;


	select chf.CALL_DATE
		   , chf.CUSTOMER_ID
		   , MIN(chf.call_date) as min_call_date
    into #unq_day		   
	from dw_mstr_dm.dbo.TblClientStreams tcs (NOLOCK)
			inner join
		 dw_mstr_dm.dbo.CALL_HISTORY_FACT chf (NOLOCK) on tcs.Client_ID=chf.CLIENT_ID
	where tcs.Client_ID in('UBN8','UBN9','UBN11','UBN12')
		  and chf.CALL_DATE > '12/1/19'														--start of time this rpt
		  and datediff(day,chf.call_date,GETDATE())<=365									--but only show rolling 12mo
--		  and chf.call_date<dateadd(day,-DATEPART(day,getdate())+1,cast(GETDATE() as DATE))	--not current incomplete mo
		  and RIGHT_PARTY_CONTACT='Y'
	group by chf.CALL_DATE
			 , chf.CUSTOMER_ID;

    create index cust on #unq_day(customer_id);
    create index mindate on #unq_day(min_call_date);
    truncate table CLIENT_ANALYTICS.dbo.client_USBank_firstRPC_day;
    
	
	with w_pay
	as
	(
	select unq_day.call_date
		   , unq_day.CUSTOMER_ID
		   , unq_day.min_call_date
		   , chf.CUSTOMER_ID as chf_customer_id
		   , MAX(chf.CALL_DATE) as max_prev
		   , sum(pf.PAYMENT_AMT_APPLIED) as PAYMENT_AMT_APPLIED
	from #unq_day unq_day
			left outer join
		 dw_mstr_dm.dbo.CALL_HISTORY_FACT chf (NOLOCK) on unq_day.CUSTOMER_ID=chf.CUSTOMER_ID
								  and unq_day.min_call_date>chf.CALL_DATE
								  and chf.RIGHT_PARTY_CONTACT='Y'
			left outer join
		 dw_mstr_dm.dbo.PAYMENT_FACT pf (NOLOCK) on unq_day.CUSTOMER_ID=pf.CUSTOMER_ID
										   and DATEDIFF(day,unq_day.min_call_date,pf.PYMT_DATE) between 0 and 1
	group by unq_day.call_date
		   , unq_day.CUSTOMER_ID
		   , unq_day.min_call_date
		   , chf.CUSTOMER_ID
	)

	insert into CLIENT_ANALYTICS.dbo.client_usbank_firstrpc_day(call_date,unq_rpcs,unq_first_rpcs,unq_first_rpcs_w_pay)
	select call_date
		   , COUNT(distinct CUSTOMER_ID) as unq_rpcs
		   , COUNT(distinct case when chf_customer_id is null then customer_id end) as unq_first_rpcs
		   , COUNT(distinct case when chf_customer_id is null and payment_amt_applied>0 then customer_id end) as unq_first_rpcs_w_pay
	from w_pay
	group by call_date


END;
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_usbank_firstrpc_day] TO [CORP\aughodake]
    AS [dbo];

