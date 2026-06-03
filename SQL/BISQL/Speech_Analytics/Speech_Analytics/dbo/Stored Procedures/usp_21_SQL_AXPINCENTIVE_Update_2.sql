

-- ====================================================
-- Author:				Vladislav Pilipets
-- Create date:			2022-08-05
-- Description:			CM_AXPINCREP updates 
-- Update 2023-04-17:	adding additional columns 
--						& logic 
-- ====================================================
CREATE PROCEDURE [dbo].[usp_21_SQL_AXPINCENTIVE_Update]
	@mail_profile		varchar(50), 
	@mail_to			varchar(1500), 
	@mail_cc			varchar(1500),  
	@exec_date			datetime = null 
AS
BEGIN

	SET NOCOUNT ON 

	DECLARE @prefix_subject AS VARCHAR(50) = (SELECT attributes FROM Speech_Analytics.dbo.cm_connattr WHERE parameter = 'prefix' AND isActive = 1) 

	--Extracting Max adte from call details table
	DECLARE @Call_Date DATETIME
	--SET @Call_Date= '2022-03-01'
	IF @exec_date IS NOT NULL 
	BEGIN 
		SET @Call_Date= @exec_date - 3
	END 
	ELSE 
	BEGIN 
		SET @Call_Date= GETDATE() - 3
	END 
	PRINT @Call_Date
	
	DECLARE @End_Month DATETIME
	
	SET @End_Month=EOMONTH(@Call_Date,0)
	PRINT @End_Month

	--pulling up all the event hits for AXP Incentive report
	IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
		DROP TABLE dbo.#Results 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#Results 
	FROM dbo.CM_EVNTSAPI 
	WHERE EOM=@End_Month AND CATHIT in ('EN AXP FRP SUC','EN AXP FRP SOL',
	'EN AXP Optima SUC','EN AXP Optima SOL','EN AXP Settle SUC',
	'EN AXP Settle SOL','EN AXP REAGE SUC','EN AXP REAGE SOL' )

	--pulling up all the Call details for AXP Incentive report
	IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
		DROP TABLE dbo.#Results1
	SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,
		DIS1,SKNM,CLDUR,DIR,RGSSEID,PHNUMB,CLNTID,
		CASE WHEN RGSACC LIKE '%-%' THEN 
			LEFT(RGSACC,LEN(RGSACC)-3) ELSE RGSACC END AS ID2,
		CASE WHEN RGSACC LIKE '%-%' 
			THEN 1 ELSE 0 END AS ACCINDICATOR
	INTO dbo.#Results1 
	FROM dbo.CM_CALLEXPORT
	WHERE EOM=@End_Month and RECDP='Veldos'
		AND AGNTID NOT IN ('5000025','5000020','5000022','5000009',
			'5000006','5000011','5000032','5000033','5000016','5000034', 
			'5000031','5000001','5000004','5000030','5000021','5000002',
			'5000012','5000026','5000014','5000027','5000024','5000007','5000019',
			'5000018','5000010','5000005','5000013','5000029','5000015','5000017',
			'5000008','5000003','5000028','5000035','5000023') 
		AND CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results)

	--Extracting CRM DETAILS for the LIVEVOX IDS
	IF OBJECT_ID('tempdb.dbo.#Resultscrm') IS NOT NULL 
		DROP TABLE dbo.#Resultscrm
	SELECT a.[number] as File_Number,
		a.[customer] AS CLNTID,
		CAST(a.[received] as date) [received],
		CAST(a.[closed] AS DATE) [closed],
		CAST(a.[returned] AS DATE) [returned],
		a.[status],a.[desk],
		a.[Branch],a.[current0],a.[id2],
		CAST(b.[ThisAgencyAgreedDate] AS DATE) [ThisAgencyAgreedDate],
		CAST(b.[ThisAgencySettlementFirstPaymentDate] AS DATE) [ThisAgencySettlementFirstPaymentDate],
		CAST(b.[ThisAgencyCompletionDate] AS DATE ) [ThisAgencyCompletionDate],
		c.[SelectedFPPStatus],CAST(c.[FPP_OfferedDate] AS DATE) [FPP_OfferedDate], 
		CAST (c.[FPP_AcceptedDate] AS DATE) [FPP_AcceptedDate],
		c.[SelectedReinstatementStatus], 
		CAST(c.[ReinstatementOfferDate] AS DATE) [ReinstatementOfferDate],
		CAST(c.[ReinstatementPifDate] AS DATE) [ReinstatementPifDate],
		CAST(c.[ReinstatementEnrolledDate] AS DATE ) [ReinstatementEnrolledDate],
		c.[OAQualified],
		CAST(d.[OADisclaimerSendDate] AS DATE) [OADisclaimerSendDate],
		d.[SelectedOAOffered],
		d.[SelectedOAAccepted]
	INTO dbo.#Resultscrm
	FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (nolock)
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_Action] b (nolock) ON a.[number]=b.[number]
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_AccountMiscData] c (nolock) ON a.[number]=c.[number]
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_RpcIndicators] d (nolock) ON a.[number]=d.[number]
	  WHERE  a.[id2] IN (SELECT DISTINCT id2 FROM dbo.#Results1 WHERE ACCINDICATOR=1)

	--Extracting CRM DETAILS for the ACTUALFILENUMBER
	IF OBJECT_ID('tempdb.dbo.#Resultscrm1') IS NOT NULL 
		DROP TABLE dbo.#Resultscrm1
	SELECT a.[number] as File_Number,
		a.[customer] AS CLNTID,
		CAST(a.[received] as date) [received],
		CAST(a.[closed] AS DATE) [closed] ,
		CAST(a.[returned] AS DATE) [returned],
		a.[status],a.[desk],
		a.[Branch],
		a.[current0],
		a.[id2],
		CAST(b.[ThisAgencyAgreedDate] AS DATE) [ThisAgencyAgreedDate],
		CAST(b.[ThisAgencySettlementFirstPaymentDate] AS DATE) [ThisAgencySettlementFirstPaymentDate],
		CAST(b.[ThisAgencyCompletionDate] AS DATE ) [ThisAgencyCompletionDate],
		c.[SelectedFPPStatus],
		CAST(c.[FPP_OfferedDate] AS DATE) [FPP_OfferedDate], 
		CAST (c.[FPP_AcceptedDate] AS DATE) [FPP_AcceptedDate],
		c.[SelectedReinstatementStatus], 
		CAST(c.[ReinstatementOfferDate] AS DATE) [ReinstatementOfferDate],
		CAST(c.[ReinstatementPifDate] AS DATE) [ReinstatementPifDate],
		CAST(c.[ReinstatementEnrolledDate] AS DATE ) [ReinstatementEnrolledDate],
		c.[OAQualified],
		CAST(d.[OADisclaimerSendDate] AS DATE) [OADisclaimerSendDate],
		d.[SelectedOAOffered],
		d.[SelectedOAAccepted]
	INTO dbo.#Resultscrm1
	FROM [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[master] a (nolock)
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_Action] b (nolock) ON a.[number]=b.[number]
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_AccountMiscData] c (nolock) ON a.[number]=c.[number]
		LEFT JOIN [HVDB02.CORPGLBDOM.LOCAL].[Amex].[dbo].[Custom_AMEX_RpcIndicators]  d (nolock) ON a.[number]=d.[number]
	  WHERE a.[number] IN (SELECT DISTINCT id2 FROM dbo.#Results1 WHERE ACCINDICATOR=0 AND ID2 not like '%.%' AND ISNUMERIC(ID2)=1)

	--final amex CRM data 
	IF OBJECT_ID('tempdb.dbo.#Resultscrm2') IS NOT NULL 
		DROP TABLE dbo.#Resultscrm2
	SELECT File_Number,CLNTID,[received],[closed],[returned],[status],[desk],
		[Branch],[current0],[id2],[ThisAgencyAgreedDate],[ThisAgencySettlementFirstPaymentDate],
		[ThisAgencyCompletionDate],[SelectedFPPStatus],[FPP_OfferedDate],[FPP_AcceptedDate],
		[SelectedReinstatementStatus],[ReinstatementOfferDate],[ReinstatementPifDate],
		[ReinstatementEnrolledDate],[OAQualified],[OADisclaimerSendDate],[SelectedOAOffered],
		[SelectedOAAccepted]
	INTO dbo.#Resultscrm2
	FROM (SELECT File_Number,CLNTID,[received],[closed],[returned],[status],[desk],
			[Branch],[current0],[id2],[ThisAgencyAgreedDate],[ThisAgencySettlementFirstPaymentDate],
			[ThisAgencyCompletionDate],[SelectedFPPStatus],[FPP_OfferedDate],[FPP_AcceptedDate],
			[SelectedReinstatementStatus],[ReinstatementOfferDate],[ReinstatementPifDate],
			[ReinstatementEnrolledDate],[OAQualified],[OADisclaimerSendDate],[SelectedOAOffered],
			[SelectedOAAccepted] FROM dbo.#Resultscrm
		UNION
			SELECT File_Number,CLNTID,[received],[closed],[returned],[status],[desk],
			[Branch],[current0],[id2],[ThisAgencyAgreedDate],[ThisAgencySettlementFirstPaymentDate],
			[ThisAgencyCompletionDate],[SelectedFPPStatus],[FPP_OfferedDate],[FPP_AcceptedDate],
			[SelectedReinstatementStatus],[ReinstatementOfferDate],[ReinstatementPifDate],
			[ReinstatementEnrolledDate],[OAQualified],[OADisclaimerSendDate],[SelectedOAOffered],
			[SelectedOAAccepted] FROM dbo.#Resultscrm1) a

	--sepearate FRP CRM INFO
	IF OBJECT_ID('tempdb.dbo.#Resultscrmfrp') IS NOT NULL 
		DROP TABLE dbo.#Resultscrmfrp
	SELECT File_Number,CLNTID,[received],[closed],[returned],[status] as ACCSTATUS,[desk] AS ACCDESK,
		[Branch] AS ACCBRNCH,[current0] AS ACCBAL,[id2] AS ACCID2,
		[SelectedFPPStatus] AS ACCSELECTEDFPPSTATUS,[FPP_OfferedDate],[FPP_AcceptedDate]
	INTO dbo.#Resultscrmfrp
	FROM dbo.#Resultscrm2
	WHERE [SelectedFPPStatus] IS NOT NULL

	--sepearate OA CRM INFO
	IF OBJECT_ID('tempdb.dbo.#ResultscrmOA') IS NOT NULL 
		DROP TABLE dbo.#ResultscrmOA
	SELECT File_Number,CLNTID,[received],[closed],[returned],[status] as ACCSTATUS,[desk] AS ACCDESK,
		[Branch] AS ACCBRNCH,[current0] AS ACCBAL,[id2] AS ACCID2,
		[OAQualified] AS ACCOAQUALIFIED,[OADisclaimerSendDate],
		[SelectedOAOffered] AS ACCSELECTEDOAOFFERED,[SelectedOAAccepted] AS ACCSELECTEDOAACCEPTED
	INTO dbo.#ResultscrmOA
	FROM dbo.#Resultscrm2
	WHERE [OAQualified] IS NOT NULL

	--sepearate rns CRM INFO
	IF OBJECT_ID('tempdb.dbo.#Resultscrmrns') IS NOT NULL 
		DROP TABLE dbo.#Resultscrmrns
	SELECT File_Number,CLNTID,[received],[closed],[returned],[status] as ACCSTATUS,[desk] AS ACCDESK,
		[Branch] AS ACCBRNCH,[current0] AS ACCBAL,[id2] AS ACCID2,
		[SelectedReinstatementStatus] AS ACCSELECTEDREINSTATEMENTSTATUS,[ReinstatementOfferDate],
		[ReinstatementPifDate],[ReinstatementEnrolledDate]
	INTO dbo.#Resultscrmrns
	FROM dbo.#Resultscrm2
	WHERE [SelectedReinstatementStatus] IS NOT NULL

	--sepearate settlement CRM INFO
	IF OBJECT_ID('tempdb.dbo.#ResultscrmSET') IS NOT NULL 
		DROP TABLE dbo.#ResultscrmSET
	SELECT File_Number,CLNTID,[received],[closed],
		[returned],[status] as ACCSTATUS,[desk] AS ACCDESK,
		[Branch] AS ACCBRNCH,[current0] AS ACCBAL,[id2] AS ACCID2,
		[ThisAgencyAgreedDate],[ThisAgencySettlementFirstPaymentDate],
		[ThisAgencyCompletionDate]
	INTO dbo.#ResultscrmSET
	FROM dbo.#Resultscrm2
	WHERE [ThisAgencyAgreedDate] IS NOT NULL

	--pulling up all the call with FRP Success 
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP 
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#ResultsFRP 
	FROM dbo.#Results WHERE CATHIT='EN AXP FRP SUC'

	--pulling up all the call with FRP Solicit 
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP1') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP1 
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#ResultsFRP1 
	FROM  dbo.#Results WHERE CATHIT='EN AXP FRP SOL'

	--Unique FRP Success
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP2') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP2 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsFRP2 
	FROM dbo.#ResultsFRP WHERE RNK=1 

	--Unique FRP Solicit
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP3') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP3 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsFRP3 
	FROM dbo.#ResultsFRP1 WHERE RNK=1 

	--extracting the missed solicit FRP
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP4') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP4 
	Select a.CALLID,'EN AXP FRP SOL' as CATHIT,'FRP' AS COMPNAME,a.STTIME
	INTO dbo.#ResultsFRP4 
	FROM dbo.#ResultsFRP2 a 
		LEFT JOIN dbo.#ResultsFRP3 b on a.CALLID=b.CALLID
	WHERE b.CALLID IS NULL

	--Final Solicit FRP
	--Final Solicit FRP
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP5') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP5
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsFRP5
	FROM (SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#ResultsFRP3
	UNION 
	SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#ResultsFRP4) a

	--FRP Clinet ID calculation 
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP6') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP6
	SELECT a.CALLID,a.STTIME AS SOLTIME,1 as CLLCNT,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE 1 END AS CXSUCC,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE b.STTIME END AS SUCTIME,'FRP' AS REPTYPE,
		CASE WHEN c.CLNTID IN ('111CDDR','113HIGR','116ALLR','117ALLR','118ALLR','118MIDR',
		'119ALLR','119IUDR','119MD1R','119MD2R','119MD3R','119OOSR','121CBCA','121CBFA',
		'121CBTA','121CBXA','121EBFA','121ETCA','121LBFA','121LTCA','121LTFA','121LTTA',
		'121LTXA','12EC2CA','12EC2FA','12ECOTA','R3CFM','R3PGR','R3POR','R3PSR','R3PTR',
		'RA15V','RA1BM','RA1NM','RA1TM','RA1XM','RA1YM','RA25V','RA2BM','RA2NM','RA2TM',
		'RA2XM','RA2YM','RA35V','RA36V','RA3CM','RA3MM','RA3NM','RA3TM','RA3XM','RA3YM',
		'RA43V','RA46V','RA49V','RA4AM','RA4BM','RA4CM','RA4DM','RA4IV','RA4LM','RA4MM',
		'RA4NM','RA4OV','RA4PM','RA4QM','RA4RM','RA4SM','RA4TM','RA4UV','RA4YM','RA5BM',
		'RA5CM','RA5FM','RA5GM','RA5JM','RA5MM','RA5NM','RA5OM','RA5PM','RA5SM','RA5TM',
		'RA5YM','RA65M','RA66M','RA67M','RA68M','RA6AM','RA6CM','RA6FM','RA6JM','RA6MM',
		'RA6NM','RA6OM','RA6RM','RA6TM','RA6XM','RA6YM','RA77M','RA78M','RA79M','RA7AM',
		'RA7BM','RA7CM','RA7EM','RA7GM','RA7JM','RA7KM','RA7MM','RA7PM','RA7QM','RA7RM',
		'RA7TM','RA7WM','RA7XM','RA7YM','RA8JM','RA8MM','RA8SM','RAI9M','RAIAM','RAIBM',
		'RAICM','RAIEM','RAIHM','RAIJM','RAIKM','RAILM','RAIMM','RAINM','RAIPM','RAITM',
		'RALOM','RAPLM','RARAM','RARTM','RARXM','RARYM','RCGAR','RLK1M','RLK2M','RLK3M',
		'RLK4M','RLT1M','RLUJM','12LJ2AR','12LJ2JR','12LJ2ZR','113CFRR','113LFRR','113OFRR',
		'113SFRR','121DSPA','121EBCA','121EBTA','121EBXA','121ETFA','121ETTA','121ETXA',
		'121HBCA','121HBFA','121HBTA','121HBXA','121LBCA','121LBTA','121LBXA','121PLAA',
		'1221BCA','1221BFA','1221BXA','1221TCA','1221TFA','1221TXA','12260CA','12260FA','12260TA',
		'12260XA','122ACNA','122EJSA','122LBCA','122LBFA','122LBTA','122LBXA','122LTCA',
		'122LTFA','122LTTA','122LTXA','122OTHA','123D2AA','123D2CA','123D2FA','123LBCA',
		'123LBFA','123LBTA','123LBXA','123LTCA','123LTFA','123LTTA','123LTXA','12EACNA',
		'12EC2AA','12EC2TA','12ECOCA','12ECOFA','12ECOXA','113HI3R','113HI1R','3CFM','3PGR',
		'3POR','3PSR','3PTR','A15V','A1BM','A1NM','A1TM','A1XM','A1YM','A25V','A2BM','A2NM',
		'A2TM','A2XM','A2YM','A35V','A36V','A3CM','A3MM','A3NM','A3TM','A3XM','A3YM','A43V',
		'A46V','A49V','A4AM','A4BM','A4CM','A4DM','A4IV','A4LM','A4MM','A4NM','A4OV','A4PM',
		'A4QM','A4RM','A4SM','A4TM','A4UV','A4YM','A5BM','A5CM','A5FM','A5GM','A5JM','A5MM',
		'A5NM','A5OM','A5PM','A5SM','A5TM','A5YM','A65M','A66M','A67M','A68M','A6AM','A6CM',
		'A6FM','A6JM','A6MM','A6NM','A6OM','A6RM','A6TM','A6XM','A6YM','A77M','A78M','A79M',
		'A7AM','A7BM','A7CM','A7EM','A7GM','A7JM','A7KM','A7MM','A7PM','A7QM','A7RM','A7TM',
		'A7WM','A7XM','A7YM','A8JM','A8MM','A8SM','AI9M','AIAM','AIBM','AICM','AIEM','AIHM',
		'AIJM','AIKM','AILM','AIMM','AINM','AIPM','AITM','ALOM','APLM','ARAM','ARTM','ARXM',
		'ARYM','CGAR','LK1M','LK2M','LK3M','LK4M','LT1M','LUJM','RA6SM','RA6EM','RAZUM','RA5DM',
		'121GCOA','121PABA','RL1AM','RLJ1M','RLJ3M','RLJ4M','RLT2M','RLNAR','116SYCR','116SYTR',
		'117SYCR','117SYTR','118SYCR','118SYTR','119SY0R','119SY1R','119SY2R','119SY3R','119SY4R',
		'119SY5R') THEN 0 ELSE 1 END AS CLINTIDCHK
	INTO dbo.#ResultsFRP6
	FROM dbo.#ResultsFRP5 a
		LEFT JOIN dbo.#ResultsFRP2  b on a.CALLID=b.CALLID
		LEFT JOIN dbo.#Results1 c on a.CALLID=c.CALLID

	--FRP Lanuage Final
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP7') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP7
	SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE
	INTO dbo.#ResultsFRP7
	FROM dbo.#ResultsFRP6 
	WHERE CLINTIDCHK=1

	--FRP CRM Attacht Start
	IF OBJECT_ID('tempdb.dbo.#ResultsFRP8') IS NOT NULL 
		DROP TABLE dbo.#ResultsFRP8
	SELECT a.CALLID,a.SOLTIME,a.CLLCNT,a.CXSUCC,a.SUCTIME,a.REPTYPE,
		c.File_Number,c.received,c.closed,c.returned,c.ACCSTATUS,c.ACCBAL,
		C.ACCSELECTEDFPPSTATUS AS COL1,c.FPP_OfferedDate AS COL2,
		c.FPP_AcceptedDate AS COL3,NULL AS COL4
	INTO dbo.#ResultsFRP8
	FROM dbo.#ResultsFRP7 a left join dbo.#Results1 b on a.CALLID=b.CALLID
		LEFT JOIN dbo.#Resultscrmfrp c on b.ID2=c.ACCID2 and b.CLNTID=c.CLNTID
	WHERE CAST(c.ACCSELECTEDFPPSTATUS AS FLOAT)>0 AND c.returned IS NULL
	ORDER BY a.CALLID

	--pulling up all the call with Optima Success
	IF OBJECT_ID('tempdb.dbo.#ResultsOA') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#ResultsOA 
	FROM dbo.#Results WHERE CATHIT='EN AXP Optima SUC' 

	--pulling up all the call with Optima Solicit
	IF OBJECT_ID('tempdb.dbo.#ResultsOA1') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA1 
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#ResultsOA1 
	FROM dbo.#Results WHERE CATHIT='EN AXP Optima SOL'

	--Unique Optima Success
	IF OBJECT_ID('tempdb.dbo.#ResultsOA2') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA2 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsOA2 
	FROM dbo.#ResultsOA WHERE RNK=1 

	--Unique Optima Solicit
	IF OBJECT_ID('tempdb.dbo.#ResultsOA3') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA3 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsOA3 
	FROM dbo.#ResultsOA1 WHERE RNK=1 

	--extracting the missed solicit Optima
	IF OBJECT_ID('tempdb.dbo.#ResultsOA4') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA4 
	Select a.CALLID,'EN AXP Optima SOL' as CATHIT,'Optima' AS COMPNAME,a.STTIME
	INTO dbo.#ResultsOA4 
	FROM dbo.#ResultsOA2 a 
		LEFT JOIN dbo.#ResultsOA3 b on a.CALLID=b.CALLID
	WHERE b.CALLID IS NULL

	--Final Solicit Optima
	IF OBJECT_ID('tempdb.dbo.#ResultsOA5') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA5
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsOA5
	FROM (SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#ResultsOA3
			UNION 
			SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#ResultsOA4) a

	--Optima Final
	IF OBJECT_ID('tempdb.dbo.#ResultsOA6') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA6
	SELECT a.CALLID,a.STTIME AS SOLTIME,1 as CLLCNT,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE 1 END AS CXSUCC,
		CASE WHEN b.CALLID is null THEN 0 ELSE b.STTIME END AS SUCTIME,'Optima' AS REPTYPE
	INTO dbo.#ResultsOA6
	FROM dbo.#ResultsOA5 a
		LEFT JOIN dbo.#ResultsOA2  b ON a.CALLID=b.CALLID

	--Optima CRM Attacht Start
	IF OBJECT_ID('tempdb.dbo.#ResultsOA6_1') IS NOT NULL 
		DROP TABLE dbo.#ResultsOA6_1
	SELECT a.CALLID,a.SOLTIME,a.CLLCNT,a.CXSUCC,a.SUCTIME,a.REPTYPE,
		c.File_Number,c.received,c.closed,c.returned,c.ACCSTATUS,c.ACCBAL,
		C.ACCOAQUALIFIED AS COL1,c.OADisclaimerSendDate AS COL2,
		c.ACCSELECTEDOAACCEPTED  AS COL3,c.ACCSELECTEDOAOFFERED AS COL4
	INTO dbo.#ResultsOA6_1
	FROM dbo.#ResultsOA6 a 
		LEFT JOIN dbo.#Results1 b on a.CALLID=b.CALLID
		LEFT JOIN dbo.#ResultscrmOA c on b.ID2=c.ACCID2 
								AND b.CLNTID=c.CLNTID
	WHERE c.closed IS NULL 
		AND c.returned IS NULL

	--pulling up all the call with Settlement Success
	IF OBJECT_ID('tempdb.dbo.#ResultsSET') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#ResultsSET
	FROM dbo.#Results WHERE CATHIT='EN AXP Settle SUC'

	--pulling up all the call with Settlement Solicit
	IF OBJECT_ID('tempdb.dbo.#ResultsSET1') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET1 
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#ResultsSET1 
	FROM dbo.#Results WHERE CATHIT='EN AXP Settle SOL'

	--Unique Settlement Success
	IF OBJECT_ID('tempdb.dbo.#ResultsSET2') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET2 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsSET2 
	FROM dbo.#ResultsSET WHERE RNK=1 

	--Unique Settlement Solicit
	IF OBJECT_ID('tempdb.dbo.#ResultsSET3') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET3 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsSET3 
	FROM dbo.#ResultsSET1 WHERE RNK=1 

	--extracting the missed solicit Settlement
	IF OBJECT_ID('tempdb.dbo.#ResultsSET4') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET4 
	SELECT a.CALLID,'EN AXP Settle SOL' as CATHIT,'Settlement' AS COMPNAME,a.STTIME
	INTO dbo.#ResultsSET4 
	FROM dbo.#ResultsSET2 a 
		LEFT JOIN dbo.#ResultsSET3 b on a.CALLID=b.CALLID
	WHERE b.CALLID IS NULL

	--Final Solicit Settlement
	IF OBJECT_ID('tempdb.dbo.#ResultsSET5') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET5
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#ResultsSET5
	FROM (SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#ResultsSET3
		UNION 
		SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#ResultsSET4) a

	--Settlement runnerup
	IF OBJECT_ID('tempdb.dbo.#ResultsSET6') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET6
	SELECT a.CALLID,a.STTIME AS SOLTIME,1 as CLLCNT,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE 1 END AS CXSUCC,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE b.STTIME END AS SUCTIME,'Settlement' AS REPTYPE
	INTO dbo.#ResultsSET6
	FROM dbo.#ResultsSET5 a
		LEFT JOIN dbo.#ResultsSET2 b ON a.CALLID=b.CALLID

	---Settlement Final
	IF OBJECT_ID('tempdb.dbo.#ResultsSET7') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET7
	SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE
	INTO dbo.#ResultsSET7
	FROM dbo.#ResultsSET6 
	WHERE CXSUCC=1 AND SOLTIME < SUCTIME
	
	--Optima CRM Attacht Start
	IF OBJECT_ID('tempdb.dbo.#ResultsSET8') IS NOT NULL 
		DROP TABLE dbo.#ResultsSET8
	SELECT a.CALLID,a.SOLTIME,a.CLLCNT,a.CXSUCC,a.SUCTIME,a.REPTYPE,
		c.File_Number,c.received,c.closed,c.returned,c.ACCSTATUS,c.ACCBAL,
		C.ThisAgencyAgreedDate AS COL1,c.ThisAgencyCompletionDate AS COL2,
		c.ThisAgencySettlementFirstPaymentDate  AS COL3,null AS COL4
	INTO dbo.#ResultsSET8
	FROM dbo.#ResultsSET7 a 
		LEFT JOIN dbo.#Results1 b on a.CALLID=b.CALLID
		LEFT JOIN dbo.#ResultscrmSET c on b.ID2=c.ACCID2 and b.CLNTID=c.CLNTID
	WHERE c.returned IS NULL

	--pulling up all the call with Reage Success
	IF OBJECT_ID('tempdb.dbo.#Resultsrns') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#Resultsrns 
	FROM dbo.#Results WHERE CATHIT='EN AXP REAGE SUC'

	--pulling up all the call with Reage Solicit
	IF OBJECT_ID('tempdb.dbo.#Resultsrns1') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns1 
	SELECT CALLID,CATHIT,COMPNAME,ROUND(STTIME/1000,0) AS STTIME,
		ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) RNK
	INTO dbo.#Resultsrns1 
	FROM dbo.#Results WHERE CATHIT='EN AXP REAGE SOL'

	--Unique Reage Success
	IF OBJECT_ID('tempdb.dbo.#Resultsrns2') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns2 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#Resultsrns2 
	FROM dbo.#Resultsrns WHERE RNK=1 

	--Unique Reage Solicit
	IF OBJECT_ID('tempdb.dbo.#Resultsrns3') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns3 
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#Resultsrns3 
	FROM dbo.#Resultsrns1 WHERE RNK=1 

	--extracting the missed solicit Reage
	IF OBJECT_ID('tempdb.dbo.#Resultsrns4') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns4 
	SELECT a.CALLID,'EN AXP REAGE SOL' as CATHIT,'Reinstatement' AS COMPNAME,a.STTIME
	INTO dbo.#Resultsrns4 
	FROM dbo.#Resultsrns2 a 
		LEFT JOIN dbo.#Resultsrns3 b on a.CALLID=b.CALLID
	WHERE b.CALLID IS NULL

	--Final Solicit Reage
	IF OBJECT_ID('tempdb.dbo.#Resultsrns5') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns5
	SELECT CALLID,CATHIT,COMPNAME,STTIME
	INTO dbo.#Resultsrns5
	FROM (SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#Resultsrns3
	UNION 
	SELECT CALLID,CATHIT,COMPNAME,STTIME FROM dbo.#Resultsrns4) a

	--Reage Final
	IF OBJECT_ID('tempdb.dbo.#Resultsrns6') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns6
	SELECT a.CALLID,a.STTIME AS SOLTIME,1 as CLLCNT,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE 1 END AS CXSUCC,
		CASE WHEN b.CALLID IS NULL THEN 0 ELSE b.STTIME END AS SUCTIME,'Reinstatement' AS REPTYPE
	INTO dbo.#Resultsrns6
	FROM dbo.#Resultsrns5 a
		LEFT JOIN dbo.#Resultsrns2 b ON a.CALLID=b.CALLID

	--Optima CRM Attacht Start
	IF OBJECT_ID('tempdb.dbo.#Resultsrns7') IS NOT NULL 
		DROP TABLE dbo.#Resultsrns7
	SELECT a.CALLID,a.SOLTIME,a.CLLCNT,a.CXSUCC,a.SUCTIME,a.REPTYPE,
		c.File_Number,c.received,c.closed,c.returned,c.ACCSTATUS,c.ACCBAL,
		C.ACCSELECTEDREINSTATEMENTSTATUS AS COL1,c.ReinstatementOfferDate AS COL2,
		c.ReinstatementEnrolledDate  AS COL3,c.ReinstatementPifDate AS COL4
	INTO dbo.#Resultsrns7
	FROM dbo.#Resultsrns6 a 
		LEFT JOIN dbo.#Results1 b ON a.CALLID=b.CALLID
		LEFT JOIN dbo.#Resultscrmrns c ON b.ID2=c.ACCID2 
										AND b.CLNTID=c.CLNTID
	WHERE c.returned IS NULL 
		AND (c.ACCSELECTEDREINSTATEMENTSTATUS IS NOT NULL 
			AND CAST(C.ACCSELECTEDREINSTATEMENTSTATUS AS float)*1>0)

	--All Combine report
	IF OBJECT_ID('tempdb.dbo.#Resultscmbnied') IS NOT NULL 
		DROP TABLE dbo.#Resultscmbnied
	SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE,COL1,COL2,COL3,COL4,
		File_Number,received,closed,returned,ACCSTATUS,ACCBAL 
	INTO dbo.#Resultscmbnied
	FROM 
		(SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE,CAST(COL1 AS nvarchar) COL1,
			CAST(COL2 AS nvarchar) COL2,CAST(COL3 AS nvarchar) COL3,CAST(COL4 AS nvarchar) COL4,
			File_Number,received,closed,returned,ACCSTATUS,ACCBAL 
		FROM dbo.#ResultsFRP8
		UNION
		SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE,CAST(COL1 AS nvarchar) COL1,
			CAST(COL2 AS nvarchar) COL2,CAST(COL3 AS nvarchar) COL3,CAST(COL4 AS nvarchar) COL4,
			File_Number,received,closed,returned,ACCSTATUS,ACCBAL
		FROM dbo.#ResultsOA6_1
		UNION
		SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE,CAST(COL1 AS nvarchar) COL1,
			CAST(COL2 AS nvarchar) COL2,CAST(COL3 AS nvarchar) COL3,CAST(COL4 AS nvarchar) COL4,
			File_Number,received,closed,returned,ACCSTATUS,ACCBAL  
		FROM dbo.#ResultsSET8
		UNION
		SELECT CALLID,SOLTIME,CLLCNT,CXSUCC,SUCTIME,REPTYPE,CAST(COL1 AS nvarchar) COL1,
			CAST(COL2 AS nvarchar) COL2,CAST(COL3 AS nvarchar) COL3,CAST(COL4 AS nvarchar) COL4,
			File_Number,received,closed,returned,ACCSTATUS,ACCBAL  
		FROM dbo.#Resultsrns7 ) a

	IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
		DROP TABLE dbo.#Results2
	SELECT a.EOM,a.AGNTID,a.CALLID,a.CALLDT,a.RECDP,a.RGSACC,a.DIS1,
		a.SKNM,a.CLDUR,a.DIR,a.RGSSEID,a.PHNUMB,a.CLNTID,b.SOLTIME,
		b.SUCTIME,b.CLLCNT,b.CXSUCC,b.REPTYPE,b.File_Number,b.received,b.closed,b.returned,b.
		ACCSTATUS,b.ACCBAL,b.COL1,b.COL2,b.COL3,b.COL4
	INTO dbo.#Results2
	FROM dbo.#Results1 a 
		LEFT JOIN dbo.#Resultscmbnied b ON a.CALLID=b.CALLID
	WHERE b.REPTYPE IS NOT NULL
	ORDER BY b.REPTYPE 

	--delete if by mistake code rerunned
	DELETE FROM dbo.CM_AXPINCREP
	WHERE EOM=@End_Month 

	--inserting the ouput for reporting purpose
	INSERT INTO dbo.CM_AXPINCREP
	(EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,
		SKNM,CLDUR,DIR,RGSSEID,PHNUMB,CLNTID,SOLTIME,
		SUCTIME,CLLCNT,CXSUCC,REPTYPE,File_Number,received,
		closed,returned,ACCSTATUS,ACCBAL,COL1,COL2,COL3,COL4)
	SELECT EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,
		SKNM,CLDUR,DIR,RGSSEID,PHNUMB,CLNTID,SOLTIME,
		SUCTIME,CLLCNT,CXSUCC,REPTYPE,File_Number,received,
		closed,returned,ACCSTATUS,ACCBAL,COL1,COL2,COL3,COL4
	FROM dbo.#Results2
	WHERE UPPER(AGNTID) NOT LIKE '%INTERAC%'

	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results2
	PRINT @totcntmm

	DECLARE @query1 VARCHAR (MAX); 
			SET @query1 = 'select EOM,AGNTID,CALLID,CALLDT,RECDP,RGSACC,DIS1,
			SKNM,CLDUR,DIR,RGSSEID,PHNUMB,CLNTID,SOLTIME,SUCTIME,CLLCNT,
			CXSUCC,REPTYPE,File_Number,received,closed,returned,ACCSTATUS,
			ACCBAL,COL1,COL2,COL3,COL4  from dbo.CM_AXPINCREP WHERE EOM='
			+''''+convert(varchar,@End_Month,23)+'''';
	PRINT @query1

	DECLARE @folder VARCHAR(255) = '\\dfw2-bisql-001\SSISFlatFileStage\CallMiner\' + CONVERT(VARCHAR,CAST(GETDATE() AS DATE),112)
	DECLARE @cmd varchar(300) = 'mkdir ' + @folder; 
	EXEC master..xp_cmdshell @cmd 

	DECLARE @queryout varchar(8000); 
	SET @queryout = 'bcp "' + @query1 + '" queryout ' + @folder + '\AXPINCREP.csv -c -t, -T -S'
	EXEC master..xp_cmdshell @queryout 

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	AXP Incentive reporting SQL code executed for '+convert(varchar,@Call_Date,102)+'.
		
	Total count of call ids added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = isnull(@prefix_subject,'') + 'AXp Incentive reporting SQL code executed for ' + convert(varchar,@Call_Date,102);
			print @subject1
		--send email
		if (SELECT count(CALLID) FROM dbo.#Results2)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'dw@radiusgs.com;business.analytics@radiusgs.com',
			@copy_recipients=@mail_cc,
			@subject = @subject1,
			@body = @body1,
			@query = @query1 ,
			@execute_query_database='SPEECH_ANALYTICS',
			@query_result_header=1, @attach_query_result_as_file=1
		   ,@query_attachment_filename='AXPINCREP.csv'
		   ,@query_result_separator=@tab
		   ,@query_result_no_padding=1 
		   ,@query_result_width=32767; 

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_21_SQL_AXPINCENTIVE_Update] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_21_SQL_AXPINCENTIVE_Update] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_21_SQL_AXPINCENTIVE_Update] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_21_SQL_AXPINCENTIVE_Update] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_21_SQL_AXPINCENTIVE_Update] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[usp_21_SQL_AXPINCENTIVE_Update] TO [CORP\mhuang]
    AS [dbo];

