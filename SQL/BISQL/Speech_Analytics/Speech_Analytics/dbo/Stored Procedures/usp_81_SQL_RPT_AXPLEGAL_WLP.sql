


CREATE PROCEDURE [dbo].[usp_81_SQL_RPT_AXPLEGAL_WLP]
AS
BEGIN
-- Drop existing temp table if it exists
IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
	DROP TABLE dbo.#Results 
-- Create the base dataset combining accounts, court cases, desk info, statuses, payment info, etc.
SELECT   a.[State],
			-- Convert the date to Month-Year format
			FORMAT(a.received, 'MMM-yy') AS [Month],
			-- File/account identifiers
			a.number,
			a.customer,
			-- Format account number to mask first two digits
			'XX#' + RIGHT(CAST(a.account AS VARCHAR), LEN(CAST(a.account AS VARCHAR)) - 2) AS account,
			-- Account and court case status
			a.[status],
			-- Combine desk code with name (remove newlines and commas for clarity)
			CAST(a.desk as varchar(max)) + ' - ' + REPLACE(REPLACE(CAST(c.[name] as nvarchar(max)), CHAR(10), ''), ',', ' ') AS desk,

			-- Status lookup mapping court case status codes to descriptions
			CAST(b.[status] as varchar(10)) + ' - ' + REPLACE(REPLACE(CAST(h.[Description] as nvarchar(max)), CHAR(10), ''), ',', ' ') AS [Status Lookup],

			-- Amounts
			a.original,
			a.current0,

			-- Important account dates
			CAST(a.received as date) AS received,
			CAST(a.lastpaid as date) AS lastpaid,
			CASE WHEN a.closed IS NOT NULL THEN CAST(a.closed as date) ELSE CAST(a.returned as date) END  AS closed,
			CAST(a.contacted as date) AS contacted,

			-- Calculate amount paid = original - current
			COALESCE(K.paidtot, 0) AS paid,

			-- Assign row numbers per account based on most recent received date
			ROW_NUMBER() OVER (PARTITION BY a.[account] ORDER BY a.[received] DESC) AS rnk,

			-- Link-level balance from grouped dataset
			d.linkbalance,

			-- Identify WLP and NON-WLP accounts based on agency grouping
			ISNULL(e.AGCY_GRP, 'NON-WLP') AS AGCY_GRP,
			ISNULL(e.AGCY_ACTGRP, 'NON-WLP') AS AGCY_ACTGRP,

			-- Email consent status
			f.ConsentStatus,

			-- Last RPC contact date
			g.RPCDate,

			-- Latest payment date and amount
			i.PayDate,
			i.paidamount,

			-- Total PDC amount (post-dated cheques)
			j.amount AS pdcamount

		INTO dbo.#Results
		FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)

		-- Join CourtCases to get legal status
		JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[CourtCases] b (NOLOCK)
			ON a.number = b.[AccountID]

		-- Join CourtCaseStatus for descriptive lookup
		JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[CourtCaseStatus] h (NOLOCK)
			ON b.[Status] = h.[code]

		-- Left join to Desk info
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[desk] c (NOLOCK)
			ON a.desk = c.code

		-- Left join on link to calculate link-level balances
		LEFT JOIN (
				SELECT link,
				SUM(original) AS linkbalance
			FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
			WHERE a.[Branch] = '00001'
				AND a.received >= '2024-09-23'
				AND ISNULL(LTRIM(RTRIM(link)), '') <> ''
				AND a.link >0
			GROUP BY link
		) d ON a.link = d.link

		-- Join on client analytics to determine agency groupings
		LEFT JOIN CLIENT_ANALYTICS.dbo.CM_WLP_RCODES e (NOLOCK)
			ON a.customer = LEFT(e.AGCY_ID, LEN(e.AGCY_ID) - 1)

		-- Consent Status: Join to get consent (opt-out/bounce)
		LEFT JOIN (
			SELECT 
				aa.[Number],
				ab.[Code] + ' ' + ab.[Description] AS ConsentStatus
			FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
			JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_EMail_Consent] aa (NOLOCK)
				ON a.[Number] = aa.[Number]
			LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[AMEX_EMailConsentStatus_Lookup] ab (NOLOCK)
				ON aa.[ConsentStatusId] = ab.[UID]
			WHERE a.[Branch] = '00001'
				AND a.received >= '2024-09-23'
				AND ab.[Description] IN ('Opt-out', 'Bounce Back')
		) f ON a.[Number] = f.[Number]

		-- RPC Date Join: most recent valid RPC entry
		LEFT JOIN (
			SELECT 
				a.number,
				CAST(b.RPCDate as date) AS RPCDate
			FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
			JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_RpcIndicators] b (NOLOCK)
				ON a.number = b.number
			WHERE a.[Branch] = '00001'
				AND a.received >= '2024-09-23'
				AND b.RPCDate IS NOT NULL
		) g ON a.[Number] = g.[Number]

		-- Payment History: select latest pay date with valid payment type
		LEFT JOIN (
			SELECT 
				number,
				PayDate,
				totalpaid AS paidamount
			FROM (
				SELECT 
					a.number,
					CAST(b.entered AS date) AS PayDate,
					CASE WHEN TRIM(b.paytype) like '%Reversal%' THEN b.totalpaid*-1 ELSE b.totalpaid END totalpaid,
					ROW_NUMBER() OVER (PARTITION BY a.number ORDER BY b.entered) AS rnk
				FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
				LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[payhistory] b (NOLOCK)
					ON a.number = b.number
				WHERE a.[Branch] = '00001'
					AND a.received >= '2024-09-23'
					AND TRIM(b.paytype) IN (
						'Paid Client','Paid Client Reversal','Paid Client Reversal - NSF',
						'Paid Us','Paid Us Reversal','Paid Us Reversal - NSF'
					)
			) sub
		) i ON a.number = i.number

		-- PDC Info: Sum PDC amounts for recent deposits
		LEFT JOIN (
						SELECT number,
			SUM(amount) amount
			FROM(
			SELECT 
				a.number,
				b.amount,
				ROW_NUMBER() over (partition by b.number order by b.entered desc) rnk
			FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
			JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[pdc] b (NOLOCK)
				ON a.number = b.number
			WHERE a.[Branch] = '00001'
				AND a.received >= '2024-09-23'
				AND a.[status] IN ('PDC','PAYW')
				AND CAST(b.deposit AS date) BETWEEN GETDATE() - 1 AND EOMONTH(GETDATE() - 1, 0))sub
				WHERE rnk=1
				group by number
		) j ON a.number = j.number
		LEFT JOIN (
					SELECT a.number,
						ISNULL(b.paidup,0) - ISNULL(c.paiddown,0) paidtot
				FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
			LEFT JOIN (SELECT a.number,
						SUM(b.totalpaid) paidup
					FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
					LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[payhistory] b (NOLOCK)
						ON a.number = b.number
					WHERE a.[Branch] = '00001'
						AND a.received >= '2024-09-23'
						AND b.paytype IN ('Paid Client','Paid Us')
					GROUP BY a.number) B on a.number=b.number
			LEFT JOIN (SELECT a.number,
						SUM(b.totalpaid) paiddown
					FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (NOLOCK)
					LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[payhistory] b (NOLOCK)
						ON a.number = b.number
					WHERE a.[Branch] = '00001'
						AND a.received >= '2024-09-23'
						AND b.paytype  IN('Paid Client Reversal','Paid Client Reversal - NSF',
						'Paid Us Reversal','Paid Us Reversal - NSF')
					GROUP BY a.number ) C on a.number=c.number
			WHERE a.[Branch] = '00001'
						AND a.received >= '2024-09-23'
						AND (ISNULL(b.paidup,0) -ISNULL(c.paiddown,0))>0
			) K
				ON a.number = k.number
		-- Final filters: restrict to relevant branch and date range
		WHERE a.[Branch] = '00001'
			AND a.received >= '2024-09-23'

		ORDER BY a.received;



IF OBJECT_ID('tempdb.dbo.#Results_1') IS NOT NULL 
DROP TABLE dbo.#Results_1 
 SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,CASE WHEN linkbalance IS NOT NULL AND linkbalance>=75000 AND AGCY_GRP!='NON-WLP' THEN  'NON-WLP' ELSE AGCY_GRP END AGCY_GRP
		,CASE WHEN linkbalance IS NOT NULL AND linkbalance>=75000 AND AGCY_ACTGRP!='NON-WLP' THEN  'NON-WLP' ELSE AGCY_ACTGRP END AGCY_ACTGRP
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,pdcamount
 INTO dbo.#Results_1		
 FROM dbo.#Results
 WHERE PayDate IS NULL
	 and rnk=1

IF OBJECT_ID('tempdb.dbo.#Results_2') IS NOT NULL 
DROP TABLE dbo.#Results_2
 SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,CASE WHEN DATEDIFF(DAY,received,GETDATE()-1)>90 AND closed IS NULL AND AGCY_GRP!='NON-WLP' AND 
			[status]!='PAYW' THEN 'NON-WLP' ELSE AGCY_GRP END AGCY_GRP
		,CASE WHEN DATEDIFF(DAY,received,GETDATE()-1)>90 AND closed IS NULL AND AGCY_ACTGRP!='NON-WLP' AND 
			[status]!='PAYW' THEN 'NON-WLP' ELSE AGCY_ACTGRP END AGCY_ACTGRP
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,pdcamount
		,'No Payment' AS PAYCONTRI
INTO dbo.#Results_2
FROM dbo.#Results_1


IF OBJECT_ID('tempdb.dbo.#Results_3') IS NOT NULL 
DROP TABLE dbo.#Results_3
 SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,CASE WHEN DATEDIFF(DAY,received,closed)>90 AND closed IS NOT NULL AND AGCY_GRP!='NON-WLP' 
			THEN 'NON-WLP' ELSE AGCY_GRP END AGCY_GRP
		,CASE WHEN DATEDIFF(DAY,received,closed)>90 AND closed IS NOT NULL AND AGCY_ACTGRP!='NON-WLP' 
			 THEN 'NON-WLP' ELSE AGCY_ACTGRP END AGCY_ACTGRP 
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,pdcamount
		,CASE WHEN paid>0 AND contacted IS NULL AND RPCDate IS NULL THEN 'No Collector'
			  WHEN paid>0 AND lastpaid IS NULL THEN 'No Collector' 
			  WHEN paid>0 AND RPCDate<=lastpaid THEN 'No Collector' 
			  WHEN paid>0 AND RPCDate>lastpaid THEN 'Collector' 
			  WHEN paid>0 AND contacted<=lastpaid THEN 'No Collector'
			  WHEN paid>0 AND contacted>lastpaid THEN 'Collector'  ELSE PAYCONTRI END PAYCONTRI
		,1 as AccCount
		,CASE WHEN paid>0 THEN 1 ELSE 0 END PayerCnt
INTO dbo.#Results_3
FROM dbo.#Results_2


IF OBJECT_ID('tempdb.dbo.#Results_1_1') IS NOT NULL 
DROP TABLE dbo.#Results_1_1 
 SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,CASE WHEN linkbalance IS NOT NULL AND linkbalance>=75000 
			AND AGCY_GRP!='NON-WLP' THEN  'NON-WLP' ELSE AGCY_GRP END AGCY_GRP
		,CASE WHEN linkbalance IS NOT NULL AND linkbalance>=75000 
			AND AGCY_ACTGRP!='NON-WLP' THEN  'NON-WLP' ELSE AGCY_ACTGRP END AGCY_ACTGRP
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,pdcamount
 INTO dbo.#Results_1_1		
 FROM dbo.#Results
 WHERE PayDate IS NOT NULL

IF OBJECT_ID('tempdb.dbo.#Results_2_1') IS NOT NULL 
DROP TABLE dbo.#Results_2_1
 SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,CASE WHEN DATEDIFF(DAY,received,GETDATE()-1)>90 AND closed IS NULL AND AGCY_GRP!='NON-WLP' AND 
			[status]!='PAYW' THEN 'NON-WLP' ELSE AGCY_GRP END AGCY_GRP
		,CASE WHEN DATEDIFF(DAY,received,GETDATE()-1)>90 AND closed IS NULL AND AGCY_ACTGRP!='NON-WLP' AND 
			[status]!='PAYW' THEN 'NON-WLP' ELSE AGCY_ACTGRP END AGCY_ACTGRP
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,pdcamount
INTO dbo.#Results_2_1
FROM dbo.#Results_1_1

IF OBJECT_ID('tempdb.dbo.#Results_3_1') IS NOT NULL 
DROP TABLE dbo.#Results_3_1
SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,CASE WHEN DATEDIFF(DAY,received,closed)>90 AND closed IS NOT NULL AND AGCY_GRP!='NON-WLP' 
			THEN 'NON-WLP' ELSE AGCY_GRP END AGCY_GRP
		,CASE WHEN DATEDIFF(DAY,received,closed)>90 AND closed IS NOT NULL AND AGCY_ACTGRP!='NON-WLP' 
			 THEN 'NON-WLP' ELSE AGCY_ACTGRP END AGCY_ACTGRP 
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,pdcamount
		,1 as AccCount
		,ROW_NUMBER() over (PARTITION BY number order by PayDate) rnk
INTO dbo.#Results_3_1
FROM dbo.#Results_2_1
order by number,
		PayDate

IF OBJECT_ID('tempdb.dbo.#Results_4_1') IS NOT NULL 
DROP TABLE dbo.#Results_4_1
SELECT [State]
		,[Month]
		,number
		,customer
		,account
		,[status]
		,desk
		,[Status Lookup]
		,original
		,current0
		,received
		,lastpaid
		,closed
		,contacted
		,paid
		,linkbalance
		,AGCY_GRP
		,AGCY_ACTGRP
		,ConsentStatus
		,RPCDate
		,PayDate
		,paidamount
		,CASE WHEN pdcamount IS NOT NULL AND 
			ROW_NUMBER() over (PARTITION BY number order by PayDate)=1 THEN pdcamount ELSE 0 END pdcamount
		,1 as AccCount
		,ROW_NUMBER() over (PARTITION BY number order by PayDate) rnk
INTO dbo.#Results_4_1
FROM dbo.#Results_3_1

IF OBJECT_ID('tempdb.dbo.#Results_5_1') IS NOT NULL 
DROP TABLE dbo.#Results_5_1
SELECT a.[State]
		,a.[Month]
		,a.number
		,a.customer
		,a.account
		,a.[status]
		,a.desk
		,a.[Status Lookup]
		,d.original
		,d.current0
		,a.received
		,a.lastpaid
		,a.closed
		,a.contacted
		,d.paid
		,a.linkbalance
		,a.AGCY_GRP
		,a.AGCY_ACTGRP
		,a.ConsentStatus
		,a.RPCDate
		,a.PayDate
		,a.paidamount
		,b.PAYCONTRIN PAYCONTRI
		,c.acccount AccCount
		,CASE WHEN d.paid =0 THEN 0 else c.acccount END  PayerCnt
		,d.pdcamount
INTO dbo.#Results_5_1
FROM dbo.#Results_4_1 a
LEFT JOIN (SELECT number,CASE WHEN contacted IS NULL AND RPCDate IS NULL THEN 'No Collector'
						  WHEN lastpaid IS NULL THEN 'No Collector' 
						  WHEN RPCDate<=PayDate THEN 'No Collector' 
						  WHEN RPCDate>PayDate THEN 'Collector' 
						  WHEN contacted<=PayDate THEN 'No Collector'
						  WHEN contacted>PayDate THEN 'Collector'  
						  ELSE 'No Payment' END PAYCONTRIN 
			FROM dbo.#Results_4_1
			WHERE RNK=1) b
	on a.number=b.number
LEFT JOIN (SELECT number
				  ,round(1/cast(count(customer) as float),2) as acccount 
			from dbo.#Results_4_1
			group by number) c
	on a.number=c.number
LEFT JOIN (SELECT number
				  ,round(sum(pdcamount)/cast(count(customer) as float),2) pdcamount
				  ,round(min(original)/cast(count(customer) as float),2) original
				  ,round(min(current0)/cast(count(customer) as float),2) current0
				  ,round(min(paid)/cast(count(customer) as float),2) paid
			from dbo.#Results_4_1
			group by number) d
	on a.number=d.number

IF OBJECT_ID('tempdb.dbo.#Results_FNL') IS NOT NULL 
DROP TABLE dbo.#Results_FNL
SELECT Consumer_State
		,Placement_Month
		,File_Number
		,Agency_Codes
		,Amex_Number
		,Account_Status
		,Desk
		,Status_Lookup
		,Original_Balance
		,Current_Balance
		,Placement_Date
		,Last_Pay_Date
		,Closed_Date
		,Contact_Date
		,Total_Paid
		,Link_Balance
		,Agency_Group
		,Agency_Activity_Group
		,Consent_Status
		,First_RPC_Date
		,Pay_Date
		,Amount_Paid
		,Pay_Contribution
		,Account_Count
		,PDC_Amount
		,Payer_Count
 INTO dbo.#Results_FNL
 FROM
(SELECT [State] as Consumer_State
		,[Month] as Placement_Month
		,number as File_Number
		,customer as Agency_Codes
		,account as Amex_Number
		,[status] as Account_Status
		,desk as Desk
		,[Status Lookup] as Status_Lookup
		,original as Original_Balance
		,current0 as Current_Balance
		,received as Placement_Date
		,lastpaid as Last_Pay_Date
		,closed as Closed_Date
		,contacted as Contact_Date
		,paid as Total_Paid
		,linkbalance as Link_Balance
		,AGCY_GRP as Agency_Group
		,AGCY_ACTGRP as Agency_Activity_Group
		,ConsentStatus as Consent_Status
		,RPCDate as First_RPC_Date
		,PayDate as Pay_Date
		,paidamount as Amount_Paid
		,PAYCONTRI as Pay_Contribution
		,AccCount as Account_Count
		,pdcamount as PDC_Amount
		,PayerCnt as Payer_Count
FROM dbo.#Results_5_1
UNION
SELECT [State] as Consumer_State
		,[Month] as Placement_Month
		,number as File_Number
		,customer as Agency_Codes
		,account as Amex_Number
		,[status] as Account_Status
		,desk as Desk
		,[Status Lookup] as Status_Lookup
		,original as Original_Balance
		,current0 as Current_Balance
		,received as Placement_Date
		,lastpaid as Last_Pay_Date
		,closed as Closed_Date
		,contacted as Contact_Date
		,paid as Total_Paid
		,linkbalance as Link_Balance
		,AGCY_GRP as Agency_Group
		,AGCY_ACTGRP as Agency_Activity_Group
		,ConsentStatus as Consent_Status
		,RPCDate as First_RPC_Date
		,PayDate as Pay_Date
		,paidamount as Amount_Paid
		,PAYCONTRI as Pay_Contribution
		,AccCount as Account_Count
		,pdcamount as PDC_Amount
		,PayerCnt as Payer_Count
FROM dbo.#Results_3) a

IF OBJECT_ID('tempdb.dbo.#Results_FNL_1') IS NOT NULL 
DROP TABLE dbo.#Results_FNL_1
SELECT   YT.Consumer_State
		,R. CustomMonthLabel AS Placement_Month
		,YT.File_Number
		,YT.Agency_Codes
		,YT.Amex_Number
		,YT.Account_Status
		,YT.Desk
		,YT.Status_Lookup
		,YT.Original_Balance
		,YT.Current_Balance
		,YT.Placement_Date
		,YT.Last_Pay_Date
		,YT.Closed_Date
		,YT.Contact_Date
		,YT.Total_Paid
		,YT.Link_Balance
		,YT.Agency_Group
		,YT.Agency_Activity_Group
		,YT.Consent_Status
		,YT.First_RPC_Date
		,YT.Pay_Date
		,YT.Amount_Paid
		,YT.Pay_Contribution
		,YT.Account_Count
		,YT.PDC_Amount
		,YT.Payer_Count
		,S.CustomMonthLabel as Payment_Month
INTO dbo.#Results_FNL_1
FROM #Results_FNL YT
JOIN (
SELECT FormattedMonth
	,CHAR(ASCII('A') + B.RowNum - 1) + '.' + B.FormattedMonth AS CustomMonthLabel
FROM (
SELECT FormattedMonth
	  ,SortDate
	  ,ROW_NUMBER() OVER (ORDER BY SortDate) RowNum
FROM(
 SELECT 
	DISTINCT  
        FORMAT(Placement_Date, 'MMM-yy') AS FormattedMonth
		,DATEFROMPARTS(YEAR(Placement_Date), MONTH(Placement_Date), 1) AS SortDate
    FROM #Results_FNL
	) A
	) B
) R
    ON FORMAT(YT.Placement_Date, 'MMM-yy') = R.FormattedMonth
LEFT JOIN (
SELECT FormattedMonth
	,CHAR(ASCII('A') + B.RowNum - 1) + '.' + B.FormattedMonth AS CustomMonthLabel
FROM (
SELECT FormattedMonth
	  ,SortDate
	  ,ROW_NUMBER() OVER (ORDER BY SortDate) RowNum
FROM(
 SELECT 
	DISTINCT  
        FORMAT(Pay_Date, 'MMM-yy') AS FormattedMonth
		,DATEFROMPARTS(YEAR(Pay_Date), MONTH(Pay_Date), 1) AS SortDate
    FROM #Results_FNL
	) A
	) B
) S
    ON FORMAT(YT.Pay_Date, 'MMM-yy') = S.FormattedMonth


DELETE FROM CLIENT_ANALYTICS.dbo.RPT_AXPLEGAL_WLP

INSERT INTO CLIENT_ANALYTICS.dbo.RPT_AXPLEGAL_WLP
SELECT Consumer_State
		,Placement_Month
		,File_Number
		,Agency_Codes
		,Amex_Number
		,Account_Status
		,Desk
		,Status_Lookup
		,Original_Balance
		,Current_Balance
		,Placement_Date
		,Last_Pay_Date
		,Closed_Date
		,Contact_Date
		,Total_Paid
		,Link_Balance
		,Agency_Group
		,Agency_Activity_Group
		,Consent_Status
		,First_RPC_Date
		,Pay_Date
		,Amount_Paid
		,Pay_Contribution
		,Account_Count
		,PDC_Amount
		,Payer_Count
		,Payment_Month
FROM dbo.#Results_FNL_1


DECLARE @tab char(1) = CHAR(9)

DECLARE @totcntmm VARCHAR (MAX);
SELECT @totcntmm=CAST(COUNT(File_Number) AS varchar) FROM dbo.#Results_FNL
PRINT @totcntmm

DECLARE @query1 VARCHAR (MAX); 
		SET @query1 = 'select *  from dbo.RPT_AXPLEGAL_WLP';
PRINT @query1
DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Please find attached csv for WLP Report executed for '+convert(varchar,GETDATE()-1,102)+'.
		
Total count of File Numbers added are '+ (@totcntmm) +'. 

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = ' WLP Report SQL code executed for ' + convert(varchar,GETDATE()-1,102);
		print @subject1
	--send email
	if (SELECT count(File_Number) FROM dbo.#Results_FNL)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
		@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
		@recipients = 'Susheel.Bhat@radiusgs.com;Annsneha.Gonsalves@radiusgs.com;Naved.Balouch@radiusgs.com',
		@copy_recipients='Pulkit.Jain@radiusgs.com;Kalyani.Parmar@radiusgs.com;dw@radiusgs.com',
		@subject = @subject1,
		@body = @body1,
		@query = @query1 ,
		@execute_query_database='CLIENT_ANALYTICS',
		@query_result_header=1, @attach_query_result_as_file=1
	   ,@query_attachment_filename='WLP_Report_Raw.csv'
	   ,@query_result_separator=@tab
	   ,@query_result_no_padding=1 
	   ,@query_result_width=32767;

END
