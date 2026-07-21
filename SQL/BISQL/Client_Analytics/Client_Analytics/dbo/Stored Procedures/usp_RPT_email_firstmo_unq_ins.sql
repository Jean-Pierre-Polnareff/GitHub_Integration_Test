

create PROCEDURE [dbo].[usp_RPT_email_firstmo_unq_ins]

AS
-- =============================================

--  Description: repopulate a view of email performance over lifetime of emails
--
-- 	Change History:
-- 	Author			Date		Description
-- 	------------------------------------------------------
-- 	Mike Campbell   06/03/2020	Created
-- 	Mike Campbell   06/02/2021	Logic altered to use RPT_email_guid which now includes first_send_date
-- ============================================= 

BEGIN
SET NOCOUNT ON;
         
        DELETE FROM CLIENT_ANALYTICS.dbo.RPT_email_firstmo_unq
	
		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_email_firstmo_unq
		(
			first_send_event_month,
			client,
			crm,
			first_send_acccounts,
			unq_account_requested,
			unq_account_whitelist_scrubbed,
			unq_account_optout_scrubbed,
			unq_account_email_scrubbed,
			unq_account_sent,
			unq_account_bounced,
			unq_account_delivered,
			unq_account_opens,
			unq_account_optouts,
			unq_account_marked_as_spam,
			unq_account_clicks,
			payers,
			payments,
			clientid
		)
		SELECT dd.MonthDate AS first_send_event_month
			   , reg.client
			   , reg.crm
			   , COUNT(DISTINCT reg.customerid) AS first_send_accounts
			   , COUNT(DISTINCT CASE WHEN reg.requested>0 THEN reg.customerid END) AS unq_account_requested
			   , COUNT(DISTINCT CASE WHEN reg.whitelist_scrubbed>0 THEN reg.customerid END) AS unq_account_whitelist_scrubbed
			   , COUNT(DISTINCT CASE WHEN reg.optout_scrubbed>0 THEN reg.customerid END) AS unq_account_optout_scrubbed
			   , COUNT(DISTINCT CASE WHEN reg.invalid_email_scrubbed>0 THEN reg.customerid END) AS unq_account_email_scrubbed
			   , COUNT(DISTINCT CASE WHEN reg.sent>0 THEN reg.customerid END) AS unq_account_sent
			   , COUNT(DISTINCT CASE WHEN reg.bounced>0 THEN reg.customerid END) AS unq_account_bounced
			   , COUNT(DISTINCT CASE WHEN reg.delivered>0 THEN reg.customerid END) AS unq_account_delivered
			   , COUNT(DISTINCT CASE WHEN reg.unq_opens>0 THEN reg.customerid END) AS unq_account_opens
			   , COUNT(DISTINCT CASE WHEN reg.optouts>0 THEN reg.customerid END) AS unq_account_optouts
			   , COUNT(DISTINCT CASE WHEN reg.marked_as_spam>0 THEN reg.customerid END) AS unq_account_marked_as_spam
			   , COUNT(DISTINCT CASE WHEN reg.clicks>0 THEN reg.customerid END) AS unq_account_clicks
			   , COUNT(DISTINCT CASE WHEN reg.payers>0 THEN reg.customerid END) AS payers
			   , SUM(payments) AS payments
			   , reg.clientid
		FROM CLIENT_ANALYTICS.dbo.RPT_email_guid (NOLOCK) reg
		        JOIN
             DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON reg.first_send_date=dd.CalendarDate
		GROUP BY dd.MonthDate
			   , reg.client
			   , reg.crm
			   , reg.clientid;

END;
GO
GRANT EXECUTE
    ON OBJECT::[dbo].[usp_RPT_email_firstmo_unq_ins] TO [CORP\aramugade]
    AS [dbo];

