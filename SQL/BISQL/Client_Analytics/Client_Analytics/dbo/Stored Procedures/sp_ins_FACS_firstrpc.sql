USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_ins_FACS_firstrpc]    Script Date: 7/6/2023 8:19:44 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO








CREATE PROCEDURE [dbo].[sp_ins_FACS_firstrpc]

AS
/* 
Object: dbo.sp_ins_FACS_firstrpc

Author			Date		Description
Amod Ramugade	08/15/2023	Created
Amod Ramugade   10/23/2023  Added Employee_ID

*/

BEGIN
	SET NOCOUNT ON;


	select dateadd(day,-DATEPART(day,chf.CALL_DATE)+1,chf.CALL_DATE) as call_mo
		   , chf.CUSTOMER_ID
		   , chf.CLIENT_ID
		   , chf.EMPLOYEE_ID
		   , tcs.Parent
		   , tcs.Client_Stream
		   , MIN(chf.call_date) as min_call_date
    into #unq_mo		   
	from dw_mstr_dm.dbo.TblClientStreams tcs (NOLOCK)
			inner join
		 dw_mstr_dm.dbo.CALL_HISTORY_FACT chf (NOLOCK) on tcs.Client_ID=chf.CLIENT_ID
	where tcs.Parent <> 'TEST CLIENT'
	---tcs.Client_ID in('UBN8','UBN9','UBN11','UBN12')
		  and chf.CALL_DATE > '12/1/19'														--start of time this rpt
		  and datediff(day,chf.call_date,GETDATE())<=365									--but only show rolling 12mo
		  and RIGHT_PARTY_CONTACT='Y'
	group by dateadd(day,-DATEPART(day,chf.CALL_DATE)+1,chf.CALL_DATE)
			 , chf.CUSTOMER_ID
			 , chf.CLIENT_ID
			 , chf.EMPLOYEE_ID
			 , tcs.Parent
		     , tcs.Client_Stream
			 ;

    create index cust on #unq_mo(customer_id);
    create index mindate on #unq_mo(min_call_date);
    
	TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_FACS_firstrpc;
    
	
	with w_pay
	as
	(
	select unq_mo.call_mo
		   , unq_mo.CUSTOMER_ID
		   , unq_mo.CLIENT_ID
		   , unq_mo.EMPLOYEE_ID
		   , unq_mo.Parent
		   , unq_mo.Client_Stream
		   , unq_mo.min_call_date
		   , chf.CUSTOMER_ID as chf_customer_id
		   , chf.CLIENT_ID as chf_client_id
		   , chf.EMPLOYEE_ID as chf_EMPLOYEE_ID
		   , MAX(chf.CALL_DATE) as max_prev
		   , sum(pf.PAYMENT_AMT_APPLIED) as PAYMENT_AMT_APPLIED
	from #unq_mo unq_mo
			left outer join
		 dw_mstr_dm.dbo.CALL_HISTORY_FACT chf (NOLOCK) on unq_mo.CUSTOMER_ID=chf.CUSTOMER_ID
								  and unq_mo.min_call_date>chf.CALL_DATE
								  and chf.RIGHT_PARTY_CONTACT='Y'
			left outer join
		 dw_mstr_dm.dbo.PAYMENT_FACT pf (NOLOCK) on unq_mo.CUSTOMER_ID=pf.CUSTOMER_ID
										   and DATEDIFF(day,unq_mo.min_call_date,pf.PYMT_DATE) between 0 and 1
	group by unq_mo.call_mo
		   , unq_mo.CUSTOMER_ID
		   , unq_mo.CLIENT_ID
		   , unq_mo.EMPLOYEE_ID
		   , unq_mo.Parent
		   , unq_mo.Client_Stream
		   , unq_mo.min_call_date
		   , chf.CUSTOMER_ID
		   , chf.CLIENT_ID
		   , chf.EMPLOYEE_ID
	)

	insert into CLIENT_ANALYTICS.dbo.RPT_FACS_firstrpc(call_mo,client_id,employee_id,client_parent,client_stream,unq_rpcs,unq_first_rpcs,unq_first_rpcs_w_pay)
	select call_mo
	       , CLIENT_ID
		   , EMPLOYEE_ID
		   , Parent
		   , Client_Stream
		   , COUNT(distinct CUSTOMER_ID) as unq_rpcs
		   , COUNT(distinct case when chf_customer_id is null then customer_id end) as unq_first_rpcs
		   , COUNT(distinct case when chf_customer_id is null and payment_amt_applied>0 then customer_id end) as unq_first_rpcs_w_pay
	from w_pay
	group by call_mo 
	        , CLIENT_ID
			, EMPLOYEE_ID
			, Parent
		    , Client_Stream


END;







GO


