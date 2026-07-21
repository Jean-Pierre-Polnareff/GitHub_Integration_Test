

CREATE PROCEDURE [dbo].[usp_RPT_email_guid_INS_delete]
		@replaydate DATETIME = NULL
		 
AS
-- =============================================

--  Description: Insert new email activity to CLIENT_ANALYTICS.dbo.RPT_email_guid
--
-- 	Change History:
-- 	Author			Date		Description
-- 	------------------------------------------------------
-- 	Mike Campbell   07/28/2020	Created
-- =============================================
BEGIN
SET NOCOUNT ON;

	--SUMMARIZE all GUIDS with Activity after last insert_date in CLIENT_ANALYTICS.dbo.RPT_email_guid

	--guids to insert/update 
    SELECT DISTINCT guid
	INTO #guids
	FROM DW_MSTR_DM.dbo.Radius_EmailReportData emrd
	WHERE emrd.EventDate > ISNULL(@replaydate,DATEADD(DAY,-60,CAST(GETDATE() AS DATE)))
	
	CREATE INDEX #ix_guid on #guids (guid) 

	DROP TABLE IF EXISTS #sends

	--sends
	 SELECT CASE when crm=4 then 'American Express' 
	            WHEN crm=5 THEN tcs.Parent
				ELSE dcl.ClientParent
				END as client
		 , emrd.EventDate
		 , emrd.FileNumber
		 , emrd.LTR
		 , emrd.GUID
		 , dcl.ClientId
		 , max(case when EventID=10 then 1 else 0 end) as succ_send
		 , max(case when EventID=8 and emrd.[Description] in('Hard-Bounce','Hard Bounce') then 1 
		            when EventID=8 
					     and emrd.eventvalue in('INVALID_EMAIL','Blocked','BOUNCE') 
						 and emrd.vendor='Revspring' 
					     then 1
		            else 0 end) as bounce
		 , max(case when EventID=8 and emrd.[Description]='NotInWhiteList' then 1
					when EventID=13 and emrd.[Description]='NotInWhiteList' then 1 
					else 0 end) as whitelist_scrub
		 , max(case when EventID=8 and emrd.[Description]='Opt-Out' then 1 
					when EventID=13 and emrd.[Description]='Opt-Out' then 1
					else 0 end) as optout_scrub
		 , max(case when EventID=8 and emrd.[Description]='Soft-Bounce' then 1
					when EventID=13 and emrd.[Description] in('Undeliverable','Bad-Record','Bad Record') then 1
					else 0 end) as invalid_email_scrub
		 , emrd.clientsclientid
		 , CASE WHEN crm=2 THEN 'Medprod Artiva'
	            WHEN crm=3 THEN 'Thirdprod Artiva'
	            WHEN crm=4 THEN 'AMEX Latitude'
	            WHEN crm=5 THEN 'FACS'
				END AS crm 
		 , SUBSTRING(emrd.EmailAddress, CHARINDEX( '@', emrd.EmailAddress) + 1,LEN(emrd.EmailAddress)) AS email_domain
		 , emrd.CRM AS crm_nbr
		 , emrd.DomainName as SendDomain
	INTO #sends
	FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd WITH (NOLOCK)
	       JOIN #guids g WITH (NOLOCK) ON emrd.GUID=g.GUID
		   left join DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
		   left join DW_MSTR_DM.dbo.TblClientStreams tcs WITH (NOLOCK) on emrd.ClientsClientId=tcs.CLIENT_ID
	WHERE EventID in (8,10,13)
	GROUP BY CASE WHEN crm=4 THEN 'American Express' 
	              WHEN crm=5 THEN tcs.Parent
				  ELSE dcl.ClientParent
				  END
		 , emrd.EventDate
		 , emrd.FileNumber
		 , emrd.LTR
		 , emrd.GUID
		 , dcl.ClientId
		 , emrd.clientsclientid
		 , CASE WHEN crm=2 THEN 'Medprod Artiva'
	            WHEN crm=3 THEN 'Thirdprod Artiva'
	            WHEN crm=4 THEN 'AMEX Latitude'
	            WHEN crm=5 THEN 'FACS' 
				END
         ,SUBSTRING (emrd.EmailAddress, CHARINDEX( '@', emrd.EmailAddress) + 1,LEN(emrd.EmailAddress))
		 , emrd.CRM
		 , emrd.DomainName

    --first_send 
	SELECT s.crm_nbr
	       , s.FileNumber
	       , MIN(isnull(CAST(reg.send_date AS DATE),s.EventDate)) AS first_send_date
    INTO #first_send
	FROM #sends s
		    left join
		 CLIENT_ANALYTICS.dbo.RPT_email_guid reg WITH (NOLOCK) on s.crm=reg.crm   
		                                            and s.FileNumber=reg.customerid
													and s.ClientId=reg.clientid                                  
    GROUP BY s.crm_nbr
	       , s.FileNumber


	--opens
	select emrd.GUID
		 , emrd.FileNumber
		 , SUM(emrd.EventCount) AS eventcount
	INTO #opens
	FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd  WITH (NOLOCK)
	       JOIN
         #guids g  WITH (NOLOCK) ON emrd.GUID=g.GUID
		   left outer join
	   DW_MSTR_DM.dbo.DimClient dcl  WITH (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
		   left outer join
	   DW_MSTR_DM.dbo.TblClientStreams tcs  WITH (NOLOCK) on emrd.ClientsClientId=tcs.CLIENT_ID
	where EventID = 5
	GROUP BY emrd.guid
	         , emrd.filenumber

	--opens for attribution
	select emrd.GUID
	     , emrd.EventDate
		 , emrd.FileNumber
		 , emrd.ClientsClientId
		 , CASE WHEN crm=2 THEN 'Medprod Artiva'
	            WHEN crm=3 THEN 'Thirdprod Artiva'
	            WHEN crm=4 THEN 'AMEX Latitude'
	            WHEN crm=5 THEN 'FACS'
				END AS crm
		 , SUM(emrd.EventCount) AS eventcount
	INTO #opens_attr
	FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd  WITH (NOLOCK)
	       JOIN
         #guids g  WITH (NOLOCK) ON emrd.GUID=g.GUID
		   left outer join
	   DW_MSTR_DM.dbo.DimClient dcl  WITH (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
		   left outer join
	   DW_MSTR_DM.dbo.TblClientStreams tcs  WITH (NOLOCK) on emrd.ClientsClientId=tcs.CLIENT_ID
	where EventID = 5
	GROUP BY emrd.guid
	         , emrd.EventDate
			 , emrd.filenumber
			 , emrd.ClientsClientId
			 , CASE WHEN crm=2 THEN 'Medprod Artiva'
	            WHEN crm=3 THEN 'Thirdprod Artiva'
	            WHEN crm=4 THEN 'AMEX Latitude'
	            WHEN crm=5 THEN 'FACS'
				END

	--clicks
	select emrd.GUID
		 , emrd.FileNumber
		 , SUM(emrd.EventCount) AS eventcount
	INTO #clicks
	FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd  WITH (NOLOCK)
	       JOIN
         #guids g  WITH (NOLOCK) ON emrd.GUID=g.GUID
		   left outer join
	   DW_MSTR_DM.dbo.DimClient dcl  WITH (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
		   left outer join
	   DW_MSTR_DM.dbo.TblClientStreams tcs  WITH (NOLOCK) on emrd.ClientsClientId=tcs.CLIENT_ID
	where EventID=12
	GROUP BY emrd.guid
	         , emrd.filenumber

	--spam
	select emrd.GUID
		 , emrd.FileNumber
		 , SUM(emrd.EventCount) AS eventcount
	INTO #spam
	FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd  WITH (NOLOCK)
	       JOIN
         #guids g  WITH (NOLOCK) ON emrd.GUID=g.GUID
		   left outer join
	   DW_MSTR_DM.dbo.DimClient dcl  WITH (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
		   left outer join
	   DW_MSTR_DM.dbo.TblClientStreams tcs  WITH (NOLOCK) on emrd.ClientsClientId=tcs.CLIENT_ID
	where EventID IN (9,100)                                 ---7/25/2024 added eventID 100 for Blocked
	GROUP BY emrd.guid
	         , emrd.filenumber

	--optouts
	select emrd.GUID
		 , emrd.FileNumber
		 , SUM(emrd.EventCount) AS eventcount
	INTO #optout
	FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd  WITH (NOLOCK)
	       JOIN
         #guids g  WITH (NOLOCK) ON emrd.GUID=g.GUID
		   left outer join
	   DW_MSTR_DM.dbo.DimClient dcl  WITH (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
		   left outer join
	   DW_MSTR_DM.dbo.TblClientStreams tcs  WITH (NOLOCK)  on emrd.ClientsClientId=tcs.CLIENT_ID
	where EventID=6
	GROUP BY emrd.guid
	         , emrd.filenumber

	--initial pay set that associates each payment within 15 days of a campaign open to that open
	SELECT op.FileNumber
	       , op.GUID
	       , ISNULL(fcp.KeyCustomerPayment,pf.PAYMENT_FACT_ID) AS payment_id
	       , ISNULL(DATEDIFF(DAY,op.eventdate,CONVERT(DATE,CAST(fcp.KeyDate_PaymentDate AS VARCHAR(10)))),
		            DATEDIFF(DAY,op.eventdate,pf.PYMT_DATE)) AS days_from_email
		   , ISNULL(fcp.paymentappliedamt,pf.payment_amt_applied) AS paid
		   , ROW_NUMBER() OVER(PARTITION BY op.crm, op.filenumber, ISNULL(fcp.KeyCustomerPayment,pf.PAYMENT_FACT_ID)
		                       ORDER BY ISNULL(DATEDIFF(DAY,op.eventdate,CONVERT(DATE,CAST(fcp.KeyDate_PaymentDate AS VARCHAR(10)))),
		                                        DATEDIFF(DAY,op.eventdate,pf.PYMT_DATE)) ASC) AS rank_pay
           , op.EventDate
		   , dd.CalendarDate AS payment_dt
		   , pf.PYMT_DATE
		   , pf.CUSTOMER_ID AS facs_customer_id
		   , dcu.KeyCustomer AS keycustomer
		   , pf.PYMT_REF AS facs_pymt_ref
		   , op.crm
	INTO #pays0	   
	FROM #opens_attr op
		JOIN #sends se ON op.GUID=se.GUID
		LEFT JOIN DW_MSTR_DM.dbo.DimCustomer dcu  WITH (NOLOCK) ON op.FileNumber=dcu.CustomerId
										   AND op.clientsclientid=dcu.ClientId
										   AND op.crm<>'FACS'
		LEFT JOIN DW_MSTR_DM.dbo.FactCustomerPayment fcp (NOLOCK) ON dcu.KeyCustomer=fcp.keycustomer
												   AND DATEDIFF(DAY,op.eventdate,CONVERT(DATE,CAST(fcp.KeyDate_PaymentDate AS VARCHAR(10)))) BETWEEN 0 AND 15
		LEFT JOIN DW_MSTR_DM.dbo.DimPaymentType dpt  WITH (NOLOCK) ON fcp.KeyPaymentType=dpt.KeyPaymentType
		LEFT JOIN DW_MSTR_DM.dbo.PAYMENT_FACT pf  WITH (NOLOCK) ON op.filenumber=pf.CUSTOMER_ID
										   AND op.clientsclientid=pf.CLIENT_ID
										   AND DATEDIFF(DAY,op.eventdate,pf.PYMT_DATE) BETWEEN 0 AND 15
										   AND op.crm='FACS'
		LEFT JOIN DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON fcp.KeyDate_PaymentDate=dd.KeyDate
	WHERE (fcp.PaymentAppliedAmt>0
		   AND dpt.PaymentCategory='Money'
		   AND dpt.PaymentType NOT IN('DPNSF','NSF'))
		   OR
		   (pf.PAYMENT_AMT_APPLIED>0
		    AND pf.PYMT_TYPE<>'NSF')

	--nsfs for FACS identified by pymt_ref
	SELECT p0.payment_id
	       , pf.PAYMENT_AMT_APPLIED AS facs_nsf_amount
    INTO #nsf_facs
	FROM DW_MSTR_DM.dbo.PAYMENT_FACT pf  WITH (NOLOCK)
	         JOIN
         #pays0 p0 ON pf.CUSTOMER_ID=p0.facs_customer_id
		             AND pf.PYMT_REF=p0.facs_pymt_ref
					 AND pf.PAYMENT_FACT_ID<>p0.payment_id
					 AND p0.crm='FACS'
					 AND p0.rank_pay=1
    WHERE pf.PYMT_TYPE='NSF'

	--nsfs for non-FACS, which currently don't link by and ID
	--fuzzy logic to link
	SELECT p0.payment_id
	       , p0.crm
	--3/25 commenting out the negation of Amex NSFs as we fixed this in FCP table
	     --  , CASE WHEN p0.crm='AMEX Latitude' THEN -fcp.PaymentAppliedAmt 
		    --      ELSE fcp.PaymentAppliedAmt
				  --END AS nsf_amount
		   , fcp.PaymentAppliedAmt AS nsf_amount
    INTO #nsf
	FROM DW_MSTR_DM.dbo.FactCustomerPayment fcp  WITH (NOLOCK)
	         JOIN DW_MSTR_DM.dbo.DimDate dd  WITH (NOLOCK) ON fcp.KeyDate_PaymentDate=dd.KeyDate
		     JOIN DW_MSTR_DM.dbo.DimPaymentType dpt  WITH (NOLOCK) ON fcp.KeyPaymentType=dpt.KeyPaymentType
		     JOIN #pays0 p0 ON fcp.KeyCustomer=p0.keycustomer
		             AND DATEDIFF(DAY,dd.CalendarDate,p0.payment_dt)<=45
					 AND p0.crm<>'FACS'
					 and p0.rank_pay=1
					 AND p0.paid=ABS(fcp.PaymentAppliedAmt) --Amex coming in with positive NSF values and Artiva negative
						 
    WHERE dpt.PaymentType IN('DPNSF','NSF')

	--final payment summary
	SELECT p0.guid
		   , SUM(p0.paid+COALESCE(nf.facs_nsf_amount,n.nsf_amount,0)) AS paid
	INTO #pays
	FROM #pays0 p0
	       LEFT JOIN #nsf_facs nf ON p0.payment_id=nf.payment_id
		                AND p0.crm='FACS'
		   LEFT JOIN #nsf n ON p0.payment_id=n.payment_id
		          AND p0.crm=n.crm
	WHERE rank_pay=1
	GROUP BY guid           

	create index #t_sends on #sends (guid) include 
		(EventDate,crm,client,LTR,FileNumber,ClientId,ClientsClientId,succ_send,bounce,whitelist_scrub,optout_scrub,invalid_email_scrub,SendDomain) 
	create index #t_#opens on #opens (guid) include 
		(filenumber,eventcount)
	create index #t_#clicks on #clicks (guid) include (EventCount) 
	create index #t_#spam on #spam (guid) include (EventCount)
	create index #t_#optout on #optout (guid) include (EventCount) 
	create index #t_#pays on #pays (guid) include (paid)
	create index #t_#first_send on #first_send (crm_nbr, FileNumber) include (first_send_date)
	
	drop table if exists #inserts

	--summarize by guid
	select cast(s.EventDate as DATE) as send_date
		   , s.crm
		   , s.client
		   , s.LTR
		   , s.guid
		   , s.FileNumber AS customerid
		   , ISNULL(s.ClientId,s.ClientsClientId) AS clientid
		   , sum(s.succ_send+s.bounce+s.whitelist_scrub+s.optout_scrub+s.invalid_email_scrub) as requested
		   , isnull(sum(s.whitelist_scrub),0) as whitelist_scrubbed
		   , isnull(sum(s.optout_scrub),0) as optout_scrubbed
		   , isnull(sum(s.invalid_email_scrub),0) as invalid_email_scrubbed
		   , isnull(sum(s.succ_send+s.bounce),0) as sent
		   , isnull(sum(s.bounce),0) as bounced
		   , isnull(sum(s.succ_send),0) as delivered
		   , isnull(COUNT(distinct o.filenumber),0) as unq_opens
		   , isnull(sum(op.EventCount),0) as optouts
		   , isnull(sum(sp.EventCount),0) as marked_as_spam
		   , isnull(sum(case when op.EventCount is null and sp.EventCount is null
					  then c.EventCount end),0) as clicks
		   , isnull(COUNT(CASE WHEN o.eventcount>0 THEN py.guid end),0) as payers   --payers qualified with an open
		   , ISNULL(SUM(CASE WHEN o.eventcount>0 THEN py.paid end),0) AS payments    --payment qualified with an open
		   , GETDATE() AS insert_date
		   , null AS update_date
		   , s.email_domain
		   , fs.first_send_date
		   , isnull(sum(o.eventcount),0) as total_opens
		   , s.SendDomain
	INTO #inserts
	FROM #sends s WITH (NOLOCK)
		   LEFT JOIN
		 #opens o WITH (NOLOCK)  on s.GUID=o.GUID
		   LEFT JOIN
		 #clicks c WITH (NOLOCK)  on s.GUID=c.GUID
		   LEFT JOIN
		 #spam sp WITH (NOLOCK)  on s.GUID=sp.GUID     
		   LEFT JOIN
		 #optout op WITH (NOLOCK) on s.GUID=op.GUID     
		   LEFT JOIN
		 #pays py WITH (NOLOCK) on s.guid=py.guid
		   LEFT JOIN
         #first_send fs WITH (NOLOCK) ON s.crm_nbr=fs.crm_nbr
											AND s.FileNumber=fs.FileNumber
	group by cast(s.EventDate as DATE)
		   , s.crm
		   , s.client
		   , s.LTR
		   , s.guid
		   , s.FileNumber
		   , s.email_domain
		   , ISNULL(s.ClientId,s.ClientsClientId)
		   , fs.first_send_date
		   , s.SendDomain;

	 CREATE INDEX #t_#inserts_guid ON #inserts(guid) 

     --update all existing with new values 
	 UPDATE r 
	 SET [send_date] = i.[send_date]
           ,[crm] = i.[crm]
           ,[client] = i.[client]
           ,[LTR] = i.[LTR]
           ,[customerid] =i.[customerid]
           ,[clientid] = i.[clientid]
           ,[requested] = i.[requested]
           ,[whitelist_scrubbed] = i.[whitelist_scrubbed]
           ,[optout_scrubbed] = i.[optout_scrubbed]
           ,[invalid_email_scrubbed] = i.[invalid_email_scrubbed] 
           ,[sent] = i.[sent]
           ,[bounced] = i.[bounced] 
           ,[delivered] = i.[delivered] 
           ,[unq_opens] = i.[unq_opens]
           ,[optouts] = i.[optouts]
           ,[marked_as_spam] = i.[marked_as_spam] 
           ,[clicks] = i.[clicks]
           ,[payers] = i.[payers]
           ,[payments] = i.[payments]
           ,[update_date] = i.[update_date]
		   ,[email_domain] = i.[email_domain]
		   ,[first_send_date] = i.[first_send_date] 
		   ,[total_opens] = i.[total_opens]
		   ,SendDomain = i.SendDomain 
	 FROM CLIENT_ANALYTICS.dbo.RPT_email_guid r 
				JOIN #inserts i ON i.GUID = r.guid 
	 WHERE (r.[send_date] != i.[send_date]
			   OR r.[crm] != i.[crm]
			   OR r.[client] != i.[client]
			   OR r.[LTR] != i.[LTR]    
			   OR r.[customerid] !=i.[customerid] 
			   OR r.[clientid] != i.[clientid]
			   OR r.[requested] != i.[requested]
			   OR r.[whitelist_scrubbed] != i.[whitelist_scrubbed]
			   OR r.[optout_scrubbed] != i.[optout_scrubbed]
			   OR r.[invalid_email_scrubbed] != i.[invalid_email_scrubbed] 
			   OR r.[sent] != i.[sent]
			   OR r.[bounced] != i.[bounced] 
			   OR r.[delivered] != i.[delivered] 
			   OR r.[unq_opens] != i.[unq_opens]
			   OR r.[optouts] != i.[optouts]
			   OR r.[marked_as_spam] != i.[marked_as_spam] 
			   OR r.[clicks] != i.[clicks]
			   OR r.[payers] != i.[payers]
			   OR r.[payments] != i.[payments]
			   OR r.[email_domain] != i.[email_domain]
			   OR r.[first_send_date] != i.[first_send_date]  
			   OR r.[total_opens] != i.[total_opens]
			   OR r.SendDomain != i.SendDomain)

	 --insert all guids
	 INSERT INTO CLIENT_ANALYTICS.dbo.RPT_email_guid
		   ([send_date]
           ,[crm]
           ,[client]
           ,[LTR]
           ,[guid]
           ,[customerid]
           ,[clientid]
           ,[requested]
           ,[whitelist_scrubbed]
           ,[optout_scrubbed]
           ,[invalid_email_scrubbed]
           ,[sent]
           ,[bounced]
           ,[delivered]
           ,[unq_opens]
           ,[optouts]
           ,[marked_as_spam]
           ,[clicks]
           ,[payers]
           ,[payments]
           ,[insert_date]
           ,[update_date]
		   ,[email_domain]
		   ,[first_send_date]
		   ,[total_opens]
		   ,SendDomain)
	 SELECT i.[send_date]
           ,i.[crm]
           ,i.[client]
           ,i.[LTR]
           ,i.[guid]
           ,i.[customerid]
           ,i.[clientid]
           ,i.[requested]
           ,i.[whitelist_scrubbed]
           ,i.[optout_scrubbed]
           ,i.[invalid_email_scrubbed]
           ,i.[sent]
           ,i.[bounced]
           ,i.[delivered]
           ,i.[unq_opens]
           ,i.[optouts]
           ,i.[marked_as_spam]
           ,i.[clicks]
           ,i.[payers]
           ,i.[payments]
           ,i.[insert_date]
           ,i.[update_date] 
		   ,i.[email_domain]
		   ,i.[first_send_date]
		   ,i.[total_opens]
		   ,i.SendDomain
	 FROM #inserts i with (nolock) 
		LEFT JOIN CLIENT_ANALYTICS.dbo.RPT_email_guid g with (nolock) on g.GUID = i.GUID 
	WHERE g.guid IS NULL; 

END;