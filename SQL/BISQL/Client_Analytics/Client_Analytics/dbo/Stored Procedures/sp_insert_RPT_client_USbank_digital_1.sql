





CREATE PROCEDURE [dbo].[sp_insert_RPT_client_USbank_digital]
	
	@StartDateTime DATETIME = NULL
	, @end datetime =  NULL
	
AS
/* 
Object: dbo.[sp_insert_RPT_client_USbank_digital]

Description: Truncate last 2 months and replace results in RPT table

Author			Date		Description
Mike Campbell	02/03/2023	Created
*/

BEGIN
	SET NOCOUNT ON;

	select distinct MonthDate
	into #mon
	from DW_MSTR_DM.dbo.DimDate
	where CalendarDate between DATEADD(month,-1,getdate()) and getdate();
	
	delete from client_analytics.dbo.RPT_client_USbank_digital
	where rpt_month in (select * from #mon);

	with em
	as
	(
	select dd.MonthDate as rpt_month
		   , tcs.Client_ID
		   , sum(reg.sent) as emails_sent
		   , sum(reg.total_opens) as emails_opened
		   , sum(reg.optouts) as email_optouts
	from CLIENT_ANALYTICS.dbo.RPT_email_guid reg (nolock)
			join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (nolock) on reg.clientid=tcs.Client_ID
			join
		 DW_MSTR_DM.dbo.DimDate dd on reg.send_date=dd.CalendarDate
		    join
		 #mon m on dd.MonthDate=m.MonthDate
	where tcs.Parent='usbank'
	group by dd.MonthDate
		   , tcs.Client_ID
	),

	sms
	as
	(
	select dd.MonthDate as rpt_month
		   , tcs.Client_ID
		   , count(case when rc.livevox_result in('SMS MT Delivered','SMS MT Failed')
						then rc.creditor_code
						end) as sms_sent
		   , count(case when rc.livevox_result in('Consumer responded Stop to text')
						then rc.creditor_code
						end) as sms_optouts
	from dw_mstr_dm.dbo.RadiusCall rc
			join
		 DW_MSTR_DM.dbo.TblClientStreams tcs on rc.Creditor_Code=tcs.Client_ID
			join
		 DW_MSTR_DM.dbo.DimDate dd on rc.Call_Date=dd.CalendarDate
		    join
		 #mon m on dd.MonthDate=m.MonthDate
	where rc.Livevox_Result in('SMS MT Delivered','SMS MT Failed','Consumer responded Stop to text')
		  and rc.Platform_Id='DEF'
		  and tcs.parent='usbank'
	group by dd.MonthDate
		   , tcs.Client_ID
	),

	web
	as
	(
	select dd.MonthDate as rpt_month
		   , tcs.Client_ID
		   , count(distinct ww.SessionId) as web_sessions
		   , count(case when ww.OneTimePaymentAmount>0 then ww.SessionId end) as web_payment_sessions
		   , sum(ww.OneTimePaymentAmount) as web_payment_dollars
		   , count(case when ww.PaySeriesAmount>0 then ww.SessionId end) as web_recurring_payment_sessions
		   , sum(ww.PaySeriesAmount*ww.PaySeriesCount) as web_recurring_payment_dollars
	from DW_MSTR_DM.dbo.Web_Waterfall_Metrics_Data_Client ww
			join
		 DW_MSTR_DM.dbo.TblClientStreams tcs on ww.ClientId=tcs.Client_ID
			join
		 DW_MSTR_DM.dbo.DimDate dd on cast(ww.CapturedOn as date)=dd.CalendarDate
		    join
		 #mon m on dd.MonthDate=m.MonthDate
	where tcs.Parent='usbank'
	group by dd.MonthDate
		   , tcs.Client_ID
	)

    insert into client_analytics.dbo.RPT_client_USbank_digital
	     (
		   rpt_month,
		   client_id,
		   emails_sent,
		   emails_opened,
		   email_optouts,
		   sms_sent,
		   sms_optouts,
		   web_sessions,
		   web_payment_sessions,
		   web_payment_dollars,
		   web_recurring_payment_sessions,
		   web_recurring_payment_dollars,
		   InsertDate
		 )
	select coalesce(em.rpt_month,sms.rpt_month,web.rpt_month) as rpt_month
		   , coalesce(em.client_id,sms.client_id,web.client_id) as client_id
		   , em.emails_sent
		   , em.emails_opened
		   , em.email_optouts
		   , sms.sms_sent
		   , sms.sms_optouts
		   , web.web_sessions
		   , web.web_payment_sessions
		   , web.web_payment_dollars
		   , web.web_recurring_payment_sessions
		   , web.web_recurring_payment_dollars
		   , getdate() as InsertDate
	from em 
		   full join
		 sms on em.rpt_month=sms.rpt_month and em.Client_ID=sms.Client_ID
		   full join
		 web on sms.rpt_month=web.rpt_month and sms.Client_ID=web.Client_ID


END;