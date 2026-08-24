USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_insert_RPT_client_Amex_digital]    Script Date: 10/13/2021 6:06:49 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





alter PROCEDURE [dbo].[sp_insert_RPT_client_Amex_digital]
		@replaydate DATE = NULL

AS
/* 
Object: sp_insert_RPT_client_Amex_digital

Description: Insert yesterday's digital metrics for AXP.  Update web payment data for last 3 days.

Author			Date		Description
Mike Campbell	10/13/2021	Created
*/

BEGIN
	SET NOCOUNT ON;

	DECLARE @rpt_dt DATE=DATEADD(DAY,-1,CAST(GETDATE() AS date));

	/*BEGIN PULL DATA FROM AMEX DB*/
	--decile tab
	  SELECT Number, DecileTag
	  INTO #dec
	  FROM [HVDB02.CORPGLBDOM.LOCAL].Amex.dbo.Custom_AMEX_AccountMiscData
	  WHERE LEN(DecileTag)>0

	  CREATE INDEX nbr ON #dec(Number)

	--segment/placement defs from latitude
	  SELECT ClientCode
			 , StandardParentCode
			 , Description
			 , FirstPartyFlag
	  INTO #rcode
	  FROM [HVDB02.CORPGLBDOM.LOCAL].Amex.dbo.Amex_ClientCodes_LookupTable

	--rtw statuscodes from latitude
		select latitudeStatus 
		INTO #rtw
		FROM [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.AMEX_Latitude_Status_Lookup
		where RTWFlag = 1

	--firstparty sms data
		DROP TABLE IF EXISTS #fsms
	
		SELECT *
		INTO #fsms
		FROM [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.SMS_Phone_Request
		WHERE cast(SMSRequestDate as date)=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))

	--amex client_lu
		DROP TABLE IF EXISTS #client_lu
	
		SELECT * 
		INTO #client_lu
		FROM [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.Amex_ClientCodes_LookupTable 

	--icollect_letter_data
		DROP TABLE IF EXISTS #ic_ld
	
		SELECT *
		INTO #ic_ld
		FROM [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.HC_ICollect_Letter_Data
		WHERE CAST(CreateDate AS DATE)=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
			  AND EMailFlag IN ('B','E','L')

	--icollect_email_letterrequest
	---go back 5 days as letterrequest data seems to predate icollect_letter_data
		DROP TABLE IF EXISTS #ic_lr
	
		SELECT *
		INTO #ic_lr
		FROM [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.LetterRequest
		WHERE CAST(DateRequested AS DATE)>=DATEADD(DAY,-5,ISNULL(@replaydate,@rpt_dt))

	--treatments
		DROP TABLE IF EXISTS #treat

		select LatCode 
		INTO #treat
		from [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.AMEX_ICollectLetterType_Lookup
		where IsTreatmentLtr = 1 
		group by LatCode

	--custom_amex_email_letterrequest
	---go back 5 days as letterrequest data seems to predate icollect_letter_data
		DROP TABLE IF EXISTS #cael
	
		SELECT *
		INTO #cael
		FROM [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.Custom_AMEX_EMail_Letter_Request
		WHERE CAST(LetterReqDateTime AS DATE)>=DATEADD(DAY,-5,ISNULL(@replaydate,@rpt_dt))

	--first_party_letter_data
	---grab all inserted yesterday
		DROP TABLE IF EXISTS #fpld
	
		SELECT *
		INTO #fpld
		from [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.First_Party_Letter_Data
		WHERE CAST(CreateDate AS DATE)=ISNULL(@replaydate,@rpt_dt)-->=DATEADD(DAY,-1,CAST(GETDATE() AS date))

	--AMEX_EMailLetterType_Lookup
		DROP TABLE IF EXISTS #ael
	
		SELECT *
		INTO #ael
		from [HVDB02.CORPGLBDOM.LOCAL].amex.dbo.AMEX_EMailLetterType_Lookup
	/*END PULL DATA FROM AMEX DB*/


	--inv
	DROP TABLE IF EXISTS #inv

		SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
			   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
			   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
			   , r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END AS placement_level
			   , r.Description AS segment
			   , r.FirstPartyFlag
			   , dcu.ClientId AS rcode_acorn_code
			   , d.DecileTag
			   , COUNT(*) AS active_inventory
			   , SUM(dcu.InitialBalance) AS active_inventory_dlrs
			   , COUNT(rtw.LatitudeStatus) AS rtw_inventory
			   , SUM(CASE WHEN rtw.LatitudeStatus IS NOT NULL THEN dcu.InitialBalance end) AS rtw_inventory_dlrs
			   , COUNT(CASE WHEN dce.EmailStatus='Y' THEN dcu.KeyCustomer END) AS rte_inventory
			   , SUM(CASE WHEN dce.EmailStatus='Y' THEN dcu.InitialBalance end) AS rte_inventory_dlrs

		INTO #inv
		FROM DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK)
				JOIN
			 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON dcu.ClientId=dcl.ClientId
													  AND dcu.sourcesystem=dcl.sourcesystem
				LEFT JOIN
			 #dec d ON dcu.CustomerId=d.Number
				LEFT JOIN
			 #rcode r ON dcu.ClientId=r.ClientCode
				LEFT JOIN
			 #rtw rtw ON dcu.StatusCode=rtw.LatitudeStatus
				LEFT JOIN
			 DW_MSTR_DM.dbo.DimCustomerEmail dce (NOLOCK) ON dcu.KeyCustomer=dce.KeyCustomer

		WHERE (dcu.CancelDate IS NULL OR dcu.CancelDate>=ISNULL(@replaydate,@rpt_dt))  --cancelled yesterday would count in yesterday's beg of day inv
			  AND dcu.ListDate<=ISNULL(@replaydate,@rpt_dt)
			  AND dcu.SourceSystem LIKE 'Amex%'
			  AND dcu.StatusCode<>'Dw_deactivate'
			  AND LEN(dcu.ClientId)>0
		GROUP BY r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END
			   , r.Description
			   , dcu.ClientId
			   , r.FirstPartyFlag
			   , d.DecileTag


	--email
	DROP TABLE IF EXISTS #email

		SELECT x.rpt_year
			   , x.rpt_month
			   , x.rpt_date
			   , x.StandardParentCode
			   , x.placement_level
			   , x.segment
			   , x.FirstPartyFlag
			   , x.rcode_acorn_code
			   , x.DecileTag
			   , SUM(x.emails_sent) AS emails_sent
			   , SUM(x.emails_unq_accounts_sent) AS emails_unq_accounts_sent
			   , SUM(x.emails_bounced) AS emails_bounced
			   , SUM(x.emails_opened) AS emails_opened
			   , SUM(x.emails_unq_accounts_opened) AS emails_unq_accounts_opened
			   , SUM(x.emails_clicked) AS emails_clicked
			   , SUM(x.emails_unq_accounts_clicked) AS emails_unq_accounts_clicked
			   , SUM(x.emails_marked_as_spam) AS emails_marked_as_spam
			   , SUM(x.emails_optouts) AS emails_optouts
		INTO #email
		FROM (
				--3rd party
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , emrd.ClientsClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(8,10,13) THEN emrd.GUID END) AS emails_sent
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(8,10,13) THEN emrd.FileNumber END) AS emails_unq_accounts_sent
					   , COUNT(DISTINCT CASE WHEN emrd.EventID=8 AND emrd.[Description] in('Hard-Bounce','Hard Bounce') 
											 THEN emrd.GUID END) AS emails_bounced
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(5) THEN emrd.GUID END) AS emails_opened
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(5) THEN emrd.FileNumber END) AS emails_unq_accounts_opened
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(12) THEN emrd.GUID END) AS emails_clicked
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(12) THEN emrd.FileNumber END) AS emails_unq_accounts_clicked
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(9) THEN emrd.GUID END) AS emails_marked_as_spam
					   , COUNT(DISTINCT CASE WHEN emrd.EventID in(6) THEN emrd.GUID END) AS emails_optouts
				FROM dw_mstr_dm.[dbo].[Radius_EmailReportData] emrd (NOLOCK)
					   join
				   DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on emrd.ClientsClientId=dcl.ClientId
					   LEFT JOIN
				   #dec d ON emrd.FileNumber=d.Number
					   LEFT JOIN
				   #rcode r ON emrd.ClientsClientId=r.ClientCode
				where (
					   emrd.EventID in(8,10,13)		--sends
					   OR emrd.EventID=5			--opens
					   OR emrd.EventID=12			--clicks
					   OR emrd.EventID=9			--spam
					   OR emrd.EventID=6			--optout
					   )
					   AND emrd.CRM=4				--amex
					   AND CAST(emrd.EventDate AS DATE)=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , emrd.ClientsClientId
					   , d.DecileTag
				union
				--1st party A
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(DISTINCT ild.ID) AS emails_sent
					   , COUNT(DISTINCT ilr.AccountID) AS emails_unq_accounts_sent
					   , COUNT(DISTINCT CASE WHEN cael.BounceBackDate IS NOT NULL THEN ild.id end) AS emails_bounced
					   , COUNT(DISTINCT CASE WHEN cael.OpenedDate IS NOT NULL THEN ild.id END) AS emails_opened
					   , COUNT(DISTINCT CASE WHEN cael.OpenedDate IS NOT NULL THEN ilr.AccountID END) AS emails_unq_accounts_opened
					   , null AS emails_clicked
					   , null AS emails_unq_accounts_clicked
					   , null AS emails_marked_as_spam
					   , null AS emails_optouts
				FROM #ic_ld ild
					   JOIN
					 #ic_lr ilr ON ild.LatLetterRequestId=ilr.LetterRequestID
					   JOIN
					 #cael cael ON ilr.AccountID=cael.Number AND(ilr.EmailLetterRequestId=cael.UID 
																 OR ilr.LetterRequestID=cael.LatLetterRequestId)
						JOIN
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON cael.Number=dcu.CustomerId
																AND dcu.SourceSystem LIKE 'Amex%'
						join
					 #client_lu cl ON dcu.ClientId=cl.ClientCode
					   LEFT JOIN
				   #dec d ON cael.Number=d.Number
					   LEFT JOIN
				   #rcode r ON dcu.ClientId=r.ClientCode
				where ild.EMailFlag IN('B','E')
					  AND (ilr.LetterCode IN(SELECT * FROM #treat)
						   OR ilr.LetterCode like '%IRMD%')
					  AND (cl.boomerangflag=1
						   OR cl.icollectflag=1)
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
				union
				--1st party B
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(DISTINCT fpld.ID) AS emails_sent
					   , COUNT(DISTINCT ilr.AccountID) AS emails_unq_accounts_sent
					   , COUNT(DISTINCT CASE WHEN cael.BounceBackDate IS NOT NULL THEN fpld.ID end) AS emails_bounced
					   , COUNT(DISTINCT CASE WHEN cael.OpenedDate IS NOT NULL THEN fpld.id END) AS emails_opened
					   , COUNT(DISTINCT CASE WHEN cael.OpenedDate IS NOT NULL THEN ilr.AccountID END) AS emails_unq_accounts_opened
					   , null AS emails_clicked
					   , null AS emails_unq_accounts_clicked
					   , null AS emails_marked_as_spam
					   , null AS emails_optouts
				FROM #fpld fpld
					   JOIN
					 #ic_lr ilr ON fpld.LatLetterRequestId=ilr.LetterRequestID
					   JOIN
					 #cael cael ON ilr.AccountID=cael.Number AND(ilr.EmailLetterRequestId=cael.UID 
																 OR ilr.LetterRequestID=cael.LatLetterRequestId)
						JOIN
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON cael.Number=dcu.CustomerId
																AND dcu.SourceSystem LIKE 'Amex%'
						join
					 #client_lu cl ON dcu.ClientId=cl.ClientCode
					   LEFT JOIN
				   #dec d ON cael.Number=d.Number
					   LEFT JOIN
				   #rcode r ON dcu.ClientId=r.ClientCode
				where fpld.Channel='E'
					  AND ilr.LetterCode IN(SELECT * FROM #treat)
					  AND ISNULL(cl.Amex_SegmentCode,'')='PHIGH'
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
				union
				--1st party C
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(DISTINCT cael.UID) AS emails_sent
					   , COUNT(DISTINCT ilr.AccountID) AS emails_unq_accounts_sent
					   , COUNT(DISTINCT CASE WHEN cael.BounceBackDate IS NOT NULL THEN cael.UID end) AS emails_bounced
					   , COUNT(DISTINCT CASE WHEN cael.OpenedDate IS NOT NULL THEN cael.UID END) AS emails_opened
					   , COUNT(DISTINCT CASE WHEN cael.OpenedDate IS NOT NULL THEN ilr.AccountID END) AS emails_unq_accounts_opened
					   , null AS emails_clicked
					   , null AS emails_unq_accounts_clicked
					   , null AS emails_marked_as_spam
					   , null AS emails_optouts
				FROM #ic_lr ilr
					   JOIN
					 #cael cael ON ilr.AccountID=cael.Number AND(ilr.EmailLetterRequestId=cael.UID 
																 OR ilr.LetterRequestID=cael.LatLetterRequestId)
						JOIN
					 #ael ael ON cael.LetterTypeID=ael.[UID]
						join
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON cael.Number=dcu.CustomerId
																AND dcu.SourceSystem LIKE 'Amex%'
						join
					 #client_lu cl ON dcu.ClientId=cl.ClientCode
					   LEFT JOIN
				   #dec d ON cael.Number=d.Number
					   LEFT JOIN
				   #rcode r ON dcu.ClientId=r.ClientCode
				where CAST(cael.LetterReqDateTime AS DATE)=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
					  AND ael.UpdateRMSOutcome=1
					  AND (cl.boomerangflag=1
						   OR cl.icollectflag=1
						   OR ISNULL(cl.Amex_SegmentCode,'')='PHIGH')
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
			 ) x
		GROUP BY x.rpt_year
			   , x.rpt_month
			   , x.rpt_date
			   , x.StandardParentCode
			   , x.placement_level
			   , x.segment
			   , x.FirstPartyFlag
			   , x.rcode_acorn_code
			   , x.DecileTag


	--SMS

		DROP TABLE IF EXISTS #sms

		SELECT x.rpt_year
			   , x.rpt_month
			   , x.rpt_date
			   , x.StandardParentCode
			   , x.placement_level
			   , x.segment
			   , x.FirstPartyFlag
			   , x.rcode_acorn_code
			   , x.DecileTag
			   , SUM(x.sms_sent) AS sms_sent
			   , SUM(x.sms_unq_accounts_sent) AS sms_unq_accounts_sent
		INTO #sms
		FROM (	
				--3rd party
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(rc.Radius_Call_Id) AS sms_sent
					   , COUNT(distinct dcu.KeyCustomer) AS sms_unq_accounts_sent
				FROM DW_MSTR_DM.dbo.RadiusCall rc (NOLOCK)
						JOIN
					 DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK) ON rc.Session_Id=fcc.SessionId
						JOIN
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcc.KeyCustomer=dcu.KeyCustomer
						JOIN
					 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on dcu.ClientId=dcl.ClientId
						LEFT JOIN
					 #dec d ON dcu.CustomerId=d.Number
						LEFT JOIN
					 #rcode r ON dcl.ClientId=r.ClientCode
				WHERE rc.Call_Date=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
						AND rc.LV_Client_Name = 'Veldos'
						AND rc.Service_Name IN('Amex Prime Click to SMS Outbound'/*,'103_Inbound_SMS'*/)
						AND LEN(dcu.ClientId)>0
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
				UNION
				--1st party
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(f.Number) AS sms_sent
					   , COUNT(distinct dcu.KeyCustomer) AS sms_unq_accounts_sent
				FROM #fsms f
						JOIN
					 DW_MSTR_DM.dbo.DimCustomer dcu ON f.Number=dcu.CustomerId
													   AND dcu.SourceSystem LIKE 'Amex%'
						JOIN
					 #client_lu cl ON dcu.ClientId=cl.ClientCode
						LEFT JOIN
					 #rcode r ON dcu.ClientId=r.ClientCode
						LEFT JOIN
					 #dec d ON dcu.CustomerId=d.Number
				WHERE LEN(dcu.ClientId)>0
					  AND (cl.BoomerangFlag = 1 
						   OR cl.AXPLegalFlag = 1 
						   OR cl.ICollectFlag = 1 
						   OR isnull(cl.Amex_SegmentCode, '') = 'PHIGH')
				GROUP BY r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
			 ) x	
		GROUP BY x.rpt_year
			   , x.rpt_month
			   , x.rpt_date
			   , x.StandardParentCode
			   , x.placement_level
			   , x.segment
			   , x.FirstPartyFlag
			   , x.rcode_acorn_code
			   , x.DecileTag


	--letters
		DROP TABLE IF EXISTS #ltr

		SELECT x.rpt_year
			   , x.rpt_month
			   , x.rpt_date
			   , x.StandardParentCode
			   , x.placement_level
			   , x.segment
			   , x.FirstPartyFlag
			   , x.rcode_acorn_code
			   , x.DecileTag
			   , SUM(x.letters_sent) AS letters_sent
			   , SUM(x.letters_unq_accounts_sent) AS letters_unq_accounts_sent
		INTO #ltr
		FROM (	
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(fcl.KeyCustomer) AS letters_sent
					   , COUNT(distinct fcl.KeyCustomer) AS letters_unq_accounts_sent
				FROM DW_MSTR_DM.dbo.FactCustomerLetter fcl (NOLOCK)
						JOIN
					 DW_MSTR_DM.dbo.DimDate dd ON fcl.KeyDate_MailDate=dd.KeyDate
						join
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcl.KeyCustomer=dcu.KeyCustomer
						JOIN
					 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on dcu.ClientId=dcl.ClientId
						LEFT JOIN
					 #dec d ON dcu.CustomerId=d.Number
						LEFT JOIN
					 #rcode r ON dcl.ClientId=r.ClientCode
				WHERE dd.CalendarDate=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
					  AND fcl.KeySourceSystem=3
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
				UNION
				--1st party A
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(icl.ID) AS letters_sent
					   , COUNT(distinct ilr.AccountID) AS letters_unq_accounts_sent
				FROM #ic_ld icl
						JOIN
					 #ic_lr ilr ON icl.LatLetterRequestId=ilr.LetterRequestID
						join
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON ilr.AccountID=dcu.CustomerId
																AND dcu.SourceSystem LIKE 'Amex%'
						JOIN
					 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on dcu.ClientId=dcl.ClientId
						JOIN
					 #client_lu cl ON dcu.ClientId=cl.ClientCode
						LEFT JOIN
					 #dec d ON dcu.CustomerId=d.Number
						LEFT JOIN
					 #rcode r ON dcl.ClientId=r.ClientCode
				WHERE CAST(icl.CreateDate AS DATE)=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
					  AND icl.EMailFlag IN('B','L')
					  AND ilr.LetterCode IN(SELECT ilr.LetterCode FROM #treat)
					  AND (cl.BoomerangFlag=1
						   OR cl.ICollectFlag=1)
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag
				UNION
				--1st party B
				SELECT YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
					   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
					   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
					   , r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END AS placement_level
					   , r.Description AS segment
					   , r.FirstPartyFlag
					   , dcu.ClientId AS rcode_acorn_code
					   , d.DecileTag
					   , COUNT(fpld.ID) AS letters_sent
					   , COUNT(distinct ilr.AccountID) AS letters_unq_accounts_sent
				FROM #fpld fpld
						JOIN
					 #ic_lr ilr ON fpld.LatLetterRequestId=ilr.LetterRequestID
						join
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON ilr.AccountID=dcu.CustomerId
																AND dcu.SourceSystem LIKE 'Amex%'
						JOIN
					 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on dcu.ClientId=dcl.ClientId
						JOIN
					 #client_lu cl ON dcu.ClientId=cl.ClientCode
						LEFT JOIN
					 #dec d ON dcu.CustomerId=d.Number
						LEFT JOIN
					 #rcode r ON dcl.ClientId=r.ClientCode
				WHERE CAST(fpld.CreateDate AS DATE)=ISNULL(@replaydate,@rpt_dt)--DATEADD(DAY,-1,CAST(GETDATE() AS date))
					  AND fpld.Channel IN('L')
					  AND ilr.LetterCode IN(SELECT ilr.LetterCode FROM #treat)
					  AND ISNULL(cl.Amex_SegmentCode,'')='PHIGH'
				group by r.StandardParentCode
					   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
							  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
							  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
							  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
							  ELSE 'Unknown'
							  END
					   , r.Description
					   , r.FirstPartyFlag
					   , dcu.ClientId
					   , d.DecileTag     
			 )	x
		GROUP BY x.rpt_year
			   , x.rpt_month
			   , x.rpt_date
			   , x.StandardParentCode
			   , x.placement_level
			   , x.segment
			   , x.FirstPartyFlag
			   , x.rcode_acorn_code
			   , x.DecileTag


	--calls
		DROP TABLE IF EXISTS #call

		select YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
			   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
			   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
			   , r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END AS placement_level
			   , r.Description AS segment
			   , r.FirstPartyFlag
			   , dcu.ClientId AS rcode_acorn_code
			   , d.DecileTag
			   , COUNT(fcc.KeyCustomer) AS calls
			   , SUM(fcc.IsOutbound) AS calls_outbound
			   , COUNT(DISTINCT CASE WHEN fcc.IsOutbound=1 THEN fcc.KeyCustomer END) AS calls_outbound_unq_accounts
			   , SUM(fcc.IsInbound) AS calls_inbound
			   , COUNT(DISTINCT CASE WHEN fcc.IsInbound=1 THEN fcc.KeyCustomer END) AS calls_inbound_unq_accounts
			   , COUNT(CASE WHEN fcc.isinbound=1 
								 AND rc.Service_Name LIKE '%Email' 
							THEN fcc.KeyCustomer END) AS calls_inbound_email
			   , COUNT(CASE WHEN fcc.isinbound=1 
								 AND rc.Service_Name LIKE '%SMS' 
							THEN fcc.KeyCustomer END) AS calls_inbound_sms
			   , COUNT(CASE WHEN fcc.isinbound=1 
								 AND rc.Service_Name LIKE '%Letters' 
							THEN fcc.KeyCustomer END) AS calls_inbound_letter
			   , COUNT(CASE WHEN fcc.isinbound=1
								 AND rc.Service_Name NOT LIKE '%Email'
								 AND rc.Service_Name NOT LIKE '%SMS' 
								 AND rc.Service_Name NOT LIKE '%Letters' 
							THEN fcc.KeyCustomer END) AS calls_inbound_other
			   , SUM(fcc.IsRPC) AS calls_total_rpcs
			   , SUM(CASE WHEN fcc.IsOutbound=1 THEN fcc.IsRPC end) AS calls_total_rpcs_outbound
			   , SUM(CASE WHEN fcc.IsInbound=1 THEN fcc.IsRPC end) AS calls_total_rpcs_inbound
			   , COUNT(CASE WHEN fcc.isinbound=1 
								 AND rc.Service_Name LIKE '%Email' 
								 AND fcc.IsRPC=1
							THEN fcc.KeyCustomer END) AS calls_inbound_email_rpcs
			   , COUNT(CASE WHEN fcc.isinbound=1 
								 AND rc.Service_Name LIKE '%SMS' 
								 AND fcc.IsRPC=1
							THEN fcc.KeyCustomer END) AS calls_inbound_sms_rpcs
			   , COUNT(CASE WHEN fcc.isinbound=1 
								 AND rc.Service_Name LIKE '%Letters' 
								 AND fcc.IsRPC=1
							THEN fcc.KeyCustomer END) AS calls_inbound_letter_rpcs
			   , COUNT(CASE WHEN fcc.isinbound=1
								 AND rc.Service_Name NOT LIKE '%Email'
								 AND rc.Service_Name NOT LIKE '%SMS' 
								 AND rc.Service_Name NOT LIKE '%Letters' 
								 AND fcc.IsRPC=1
							THEN fcc.KeyCustomer END) AS calls_inbound_other_rpcs

		INTO #call
		FROM DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK)
				JOIN
			 DW_MSTR_DM.dbo.RadiusCall rc (NOLOCK) ON fcc.SessionId=rc.Session_Id
				JOIN
			 DW_MSTR_DM.dbo.DimDate dd ON fcc.KeyDate_CallDate=dd.KeyDate
				join
			 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcc.KeyCustomer=dcu.KeyCustomer
				JOIN
			 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on dcu.ClientId=dcl.ClientId
				LEFT JOIN
			 #dec d ON dcu.CustomerId=d.Number
				LEFT JOIN
			 #rcode r ON dcl.ClientId=r.ClientCode
		WHERE dd.CalendarDate=ISNULL(@replaydate,@rpt_dt)
			  AND fcc.KeySourceSystem=3
		group by r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END
			   , r.Description
			   , r.FirstPartyFlag
			   , dcu.ClientId
			   , d.DecileTag


	--payments
		DROP TABLE IF EXISTS #pay0

		select YEAR(ISNULL(@replaydate,@rpt_dt)) AS rpt_year
			   , MONTH(ISNULL(@replaydate,@rpt_dt)) AS rpt_month
			   , ISNULL(@replaydate,@rpt_dt) AS rpt_date
			   , r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END AS placement_level
			   , r.Description AS segment
			   , r.FirstPartyFlag
			   , dcu.ClientId AS rcode_acorn_code
			   , d.DecileTag
			   , fcp.KeyCustomerPayment
			   , fcp.KeyCustomer
			   , dd.CalendarDate AS pymt_date
			   , fcp.KeyDate_PaymentDate
			   , fcp.PaymentAppliedAmt
			   , CAST(fcp.InsertDate AS DATE) AS pymt_insertdate

		INTO #pay0
		FROM DW_MSTR_DM.dbo.FactCustomerPayment fcp (NOLOCK)
				JOIN
			 DW_MSTR_DM.dbo.DimDate dd ON fcp.KeyDate_PaymentDate=dd.KeyDate
				join
			 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcp.KeyCustomer=dcu.KeyCustomer
				JOIN
			 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on dcu.ClientId=dcl.ClientId
				LEFT JOIN
			 #dec d ON dcu.CustomerId=d.Number
				LEFT JOIN
			 #rcode r ON dcl.ClientId=r.ClientCode
				JOIN
			 DW_MSTR_DM.dbo.DimPaymentType dpt (NOLOCK) ON fcp.KeyPaymentType=dpt.KeyPaymentType
		WHERE dd.CalendarDate=ISNULL(@replaydate,@rpt_dt) --BETWEEN DATEADD(DAY,-2,ISNULL(@replaydate,@rpt_dt)) AND ISNULL(@replaydate,@rpt_dt)     --go back 2 days as sometimes lag in pay sends
			  AND fcp.KeySourceSystem=3
			  AND (dpt.PaymentCategory<>'Adjustment' OR dpt.PaymentCategory IS NULL)
			  AND dpt.PaymentType NOT IN('DA','DAR')
--			  AND CAST(fcp.InsertDate AS DATE)=CAST(GETDATE() AS date)   --payments inserted today only

--  why do we need to limit based on insertdate?
--  changed above to only grab payments for the single payment date


	--first rpcs for payers
		DROP TABLE IF EXISTS #firstrpc

		SELECT p.*
			   , fcc.KeyCustomerCall
			   , fcc.SessionId
			   , fcc.KeyDate_CallDate
			   , fcc.IsInbound
			   , fcc.IsOutbound
			   , ROW_NUMBER() OVER(PARTITION BY p.KeyCustomerPayment
								   ORDER BY fcc.KeyDate_CallDate) AS rank_calldate
		INTO #firstrpc
		FROM #pay0 p
			   LEFT JOIN
			 DW_MSTR_DM.dbo.FactCustomerCall fcc ON p.KeyCustomer=fcc.KeyCustomer
													AND p.KeyDate_PaymentDate-fcc.KeyDate_CallDate>=0
													AND fcc.IsRPC=1
		ORDER BY p.KeyCustomerPayment, fcc.KeyDate_CallDate

		DELETE FROM #firstrpc WHERE rank_calldate>1

		CREATE INDEX sid ON #firstrpc(SessionId)

	--pay final
		DROP TABLE IF EXISTS #pay

		SELECT f.rpt_year
			   , f.rpt_month
			   , f.rpt_date
			   , f.StandardParentCode
			   , f.placement_level
			   , f.segment
			   , f.FirstPartyFlag
			   , f.rcode_acorn_code
			   , f.DecileTag
			   , COUNT(f.KeyCustomerPayment) AS total_payers
			   , COUNT(CASE WHEN f.IsOutbound=1 THEN f.KeyCustomerPayment END) AS total_payers_outbound
			   , COUNT(CASE WHEN f.IsInbound=1 THEN f.KeyCustomerPayment END) AS total_payers_inbound
			   , COUNT(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name LIKE '%Email'
							THEN f.KeyCustomerPayment END) AS total_payers_inbound_email
			   , COUNT(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name LIKE '%SMS'
							THEN f.KeyCustomerPayment END) AS total_payers_inbound_sms
			   , COUNT(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name LIKE '%Letters'
							THEN f.KeyCustomerPayment END) AS total_payers_inbound_letter
			   , COUNT(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name NOT LIKE '%Email'
								 AND rc.Service_Name NOT LIKE '%SMS'
								 AND rc.Service_Name NOT LIKE '%Letters'
							THEN f.KeyCustomerPayment END) AS total_payers_inbound_other
			   , sum(f.PaymentAppliedAmt) AS total_payment_$
			   , sum(CASE WHEN f.IsOutbound=1 THEN f.PaymentAppliedAmt END) AS total_payment_$_outbound
			   , sum(CASE WHEN f.IsInbound=1 THEN f.PaymentAppliedAmt END) AS total_payment_$_inbound
			   , sum(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name LIKE '%Email'
							THEN f.PaymentAppliedAmt END) AS total_payment_$_inbound_email
			   , sum(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name LIKE '%SMS'
							THEN f.PaymentAppliedAmt END) AS total_payment_$_inbound_sms
			   , sum(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name LIKE '%Letters'
							THEN f.PaymentAppliedAmt END) AS total_payment_$_inbound_letter
			   , sum(CASE WHEN f.IsInbound=1 
								 AND rc.Service_Name NOT LIKE '%Email'
								 AND rc.Service_Name NOT LIKE '%SMS'
								 AND rc.Service_Name NOT LIKE '%Letters'
							THEN f.PaymentAppliedAmt END) AS total_payment_$_inbound_other
		INTO #pay
		FROM #firstrpc f
				LEFT JOIN
			 DW_MSTR_DM.dbo.RadiusCall rc ON f.SessionId=rc.Session_Id
		GROUP BY f.rpt_year
			   , f.rpt_month
			   , f.rpt_date
			   , f.StandardParentCode
			   , f.placement_level
			   , f.segment
			   , f.FirstPartyFlag
			   , f.rcode_acorn_code
			   , f.DecileTag


	--web
	DROP TABLE IF EXISTS #web

		select YEAR(CAST(wm.CapturedOn AS DATE)) AS rpt_year
			   , MONTH(CAST(wm.CapturedOn AS DATE)) AS rpt_month
			   , CAST(wm.CapturedOn AS DATE) AS rpt_date
			   , r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END AS placement_level
			   , r.Description AS segment
			   , r.FirstPartyFlag
			   , wm.ClientId AS rcode_acorn_code
			   , d.DecileTag
			   , SUM(CASE WHEN wm.[Key]='frictionless_login' 
							   OR LEFT(wm.[KEY],16)='reference_number' 
						  THEN 1 ELSE 0 
						  END) AS web_login_attempts
			   , count(DISTINCT CASE WHEN wm.[Key]='frictionless_login' 
										  OR LEFT(wm.[KEY],16)='reference_number' 
									 THEN wm.ReferenceNumber
									 END) AS web_login_attempts_unq
			   , SUM(CASE WHEN wm.[Key] IN('frictionless_login','pin_login_succeeded') 
						  THEN 1 ELSE 0 
						  END) AS web_login_successfuls
			   , count(DISTINCT CASE WHEN wm.[Key] IN('frictionless_login','pin_login_succeeded') 
									 THEN wm.ReferenceNumber
									 END) AS web_login_successfuls_unq
			   , SUM(CASE WHEN wm.[Key] IN('installment_detail_retrieved') 
						  THEN 1 ELSE 0 
						  END) AS web_selected_offer
			   , SUM(CASE WHEN wm.[Key] IN('installment_submitted') 
						  THEN 1 ELSE 0 
						  END) AS web_setup_plan
			   , count(DISTINCT CASE WHEN wm.payments>0
									 THEN wm.ReferenceNumber
									 END) AS web_payers_unq
			   , sum(wm.payments) AS web_payment_$
		INTO #web
		FROM CLIENT_ANALYTICS.[dbo].[Web_Waterfall_Metrics_Data_Client] wm (NOLOCK)
			   join
		   DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on wm.ClientId=dcl.ClientId
			   LEFT JOIN
		   #dec d ON SUBSTRING(wm.ReferenceNumber,5,8)=d.Number
			   LEFT JOIN
		   #rcode r ON wm.ClientId=r.ClientCode
		where ReferenceNumber LIKE '004%'	--amex
		      AND SUBSTRING(wm.ReferenceNumber,5,1) IN('0','1','2','3','4','5','6','7','8','9')
			   AND CAST(wm.CapturedOn AS DATE) BETWEEN DATEADD(DAY,-7,ISNULL(@replaydate,@rpt_dt)) AND ISNULL(@replaydate,@rpt_dt)--keep last 7 days for lagged web attribution
		group by YEAR(CAST(wm.CapturedOn AS DATE))
			   , MONTH(CAST(wm.CapturedOn AS DATE))
			   , CAST(wm.CapturedOn AS DATE)
			   , r.StandardParentCode
			   , CASE WHEN LEFT(r.StandardParentCode,1)='P' THEN 'Primary'
					  WHEN LEFT(r.StandardParentCode,1)='S' THEN 'Secondary'
					  WHEN LEFT(r.StandardParentCode,1)='T' THEN 'Tertiary'
					  WHEN LEFT(r.StandardParentCode,1)='Q' THEN 'Quads'
					  ELSE 'Unknown'
					  END
			   , r.Description
			   , r.FirstPartyFlag
			   , wm.ClientId
			   , d.DecileTag


	--master list of client attributes to insert current report date
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		INTO #master
		FROM #inv
		UNION
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		FROM #email
		union
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		FROM #sms
		union
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		FROM #ltr
		union
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		FROM #call
		union
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		FROM #pay
		union
		SELECT DISTINCT rpt_year, rpt_month, rpt_date, StandardParentCode, placement_level, segment, FirstPartyFlag, rcode_acorn_code, DecileTag
		FROM #web
		WHERE rpt_date=ISNULL(@replaydate,@rpt_dt)

    --remove data for a replaydate if needed
		DELETE FROM CLIENT_ANALYTICS.dbo.RPT_client_Amex_digital
		WHERE rpt_date=(SELECT DISTINCT rpt_date FROM #master)

	--combine all for yesterday
		INSERT INTO CLIENT_ANALYTICS.dbo.RPT_client_Amex_digital
		(
		  rpt_year,
		  rpt_month,
		  rpt_date,
		  StandardParentCode,
		  placement_level,
		  segment,
		  FirstPartyFlag,
		  rcode_acorn_code,
		  DecileTag,
          active_inventory,
          active_inventory_dlrs,
          rtw_inventory,
          rtw_inventory_dlrs,
          rte_inventory,
          rte_inventory_dlrs,
          emails_sent,
          emails_unq_accounts_sent,
          emails_bounced,
          emails_opened,
          emails_unq_accounts_opened,
          emails_clicked,
          emails_unq_accounts_clicked,
          emails_marked_as_spam,
          emails_optouts,
          sms_sent,
          sms_unq_accounts_sent,
          letters_sent,
          letters_unq_accounts_sent,
          calls,
          calls_outbound,
          calls_outbound_unq_accounts,
          calls_inbound,
          calls_inbound_unq_accounts,
          calls_inbound_email,
          calls_inbound_sms,
          calls_inbound_letter,
          calls_inbound_other,
          calls_total_rpcs,
          calls_total_rpcs_outbound,
          calls_total_rpcs_inbound,
          calls_inbound_email_rpcs,
          calls_inbound_sms_rpcs,
          calls_inbound_letter_rpcs,
          calls_inbound_other_rpcs,
          total_payers,
          total_payers_outbound,
          total_payers_inbound,
          total_payers_inbound_email,
          total_payers_inbound_sms,
          total_payers_inbound_letter,
          total_payers_inbound_other,
          [total_payment_$],
          [total_payment_$_outbound],
          [total_payment_$_inbound],
          [total_payment_$_inbound_email],
          [total_payment_$_inbound_sms],
          [total_payment_$_inbound_letter],
          [total_payment_$_inbound_other],
          [web_payment_$],
          web_login_attempts,
          web_login_attempts_unq,
          web_login_successfuls,
          web_login_successfuls_unq,
          web_selected_offer,
          web_setup_plan,
          web_payers_unq,
		  InsertDate
		)
		SELECT m.rpt_year
			   , m.rpt_month
			   , m.rpt_date
			   , m.StandardParentCode
			   , m.placement_level
			   , m.segment
			   , m.FirstPartyFlag
			   , m.rcode_acorn_code
			   , m.DecileTag
			   , i.active_inventory
			   , i.active_inventory_dlrs
			   , i.rtw_inventory
			   , i.rtw_inventory_dlrs
			   , i.rte_inventory
			   , i.rte_inventory_dlrs
			   , e.emails_sent
			   , e.emails_unq_accounts_sent
			   , e.emails_bounced
			   , e.emails_opened
			   , e.emails_unq_accounts_opened
			   , e.emails_clicked
			   , e.emails_unq_accounts_clicked
			   , e.emails_marked_as_spam
			   , e.emails_optouts
			   , s.sms_sent
			   , s.sms_unq_accounts_sent
			   , l.letters_sent
			   , l.letters_unq_accounts_sent
			   , c.calls
			   , c.calls_outbound
			   , c.calls_outbound_unq_accounts
			   , c.calls_inbound
			   , c.calls_inbound_unq_accounts
			   , c.calls_inbound_email
			   , c.calls_inbound_sms
			   , c.calls_inbound_letter
			   , c.calls_inbound_other
			   , c.calls_total_rpcs
			   , c.calls_total_rpcs_outbound
			   , c.calls_total_rpcs_inbound
			   , c.calls_inbound_email_rpcs
			   , c.calls_inbound_sms_rpcs
			   , c.calls_inbound_letter_rpcs
			   , c.calls_inbound_other_rpcs
			   , p.total_payers
			   , p.total_payers_outbound
			   , p.total_payers_inbound
			   , p.total_payers_inbound_email
			   , p.total_payers_inbound_sms
			   , p.total_payers_inbound_letter
			   , p.total_payers_inbound_other
			   , p.[total_payment_$]
			   , p.[total_payment_$_outbound]
			   , p.[total_payment_$_inbound]
			   , p.[total_payment_$_inbound_email]
			   , p.[total_payment_$_inbound_sms]
			   , p.[total_payment_$_inbound_letter]
			   , p.[total_payment_$_inbound_other]
			   , w.[web_payment_$]
			   , w.web_login_attempts
			   , w.web_login_attempts_unq
			   , w.web_login_successfuls
			   , w.web_login_successfuls_unq
			   , w.web_selected_offer
			   , w.web_setup_plan
			   , w.web_payers_unq
			   , GETDATE() AS InsertDate
		FROM #master m
				LEFT JOIN
			 #inv i ON m.rpt_date=i.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(i.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(i.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(i.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(i.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(i.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(i.DecileTag,'')
				LEFT JOIN
			 #email e ON m.rpt_date=e.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(e.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(e.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(e.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(e.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(e.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(e.DecileTag,'')
				LEFT JOIN
			 #sms s ON m.rpt_date=s.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(s.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(s.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(s.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(s.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(s.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(s.DecileTag,'')
				LEFT JOIN
			 #ltr l ON m.rpt_date=l.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(l.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(l.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(l.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(l.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(l.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(l.DecileTag,'')
				LEFT JOIN
			 #call c ON m.rpt_date=c.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(c.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(c.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(c.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(c.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(c.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(c.DecileTag,'')
				LEFT JOIN
			 #pay p ON m.rpt_date=p.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(p.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(p.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(p.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(p.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(p.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(p.DecileTag,'')
				LEFT JOIN
			 #web w ON m.rpt_date=w.rpt_date
					   AND ISNULL(m.StandardParentCode,'')=ISNULL(w.StandardParentCode,'')
					   AND ISNULL(m.placement_level,'')=ISNULL(w.placement_level,'')
					   AND ISNULL(m.segment,'')=ISNULL(w.segment,'')
					   AND ISNULL(m.FirstPartyFlag,'')=ISNULL(w.FirstPartyFlag,'')
					   AND ISNULL(m.rcode_acorn_code,'')=ISNULL(w.rcode_acorn_code,'')
					   AND ISNULL(m.DecileTag,'')=ISNULL(w.DecileTag,'')


	--update previous 7 days of web metrics since payments lag (current day will update but not change)
		UPDATE AD
		SET ad.[web_payment_$]=w.[web_payment_$],
			ad.web_payers_unq=w.web_payers_unq
		FROM CLIENT_ANALYTICS.dbo.RPT_client_Amex_digital AD
				JOIN
			 #web w ON ad.rpt_date=w.rpt_date
					   AND ISNULL(ad.StandardParentCode,'')=ISNULL(w.StandardParentCode,'')
					   AND ISNULL(ad.placement_level,'')=ISNULL(w.placement_level,'')
					   AND ISNULL(ad.segment,'')=ISNULL(w.segment,'')
					   AND ISNULL(ad.FirstPartyFlag,'')=ISNULL(w.FirstPartyFlag,'')
					   AND ISNULL(ad.rcode_acorn_code,'')=ISNULL(w.rcode_acorn_code,'')
					   AND ISNULL(ad.DecileTag,'')=ISNULL(w.DecileTag,'')




END;









GO


