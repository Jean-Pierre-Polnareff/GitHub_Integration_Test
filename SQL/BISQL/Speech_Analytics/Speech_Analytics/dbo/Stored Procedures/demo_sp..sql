CREATE PROCEDURE [dbo].[usp_78_SQL_USAALVSA_RAW]
AS
BEGIN


	DECLARE @Start_Date DATETIME
	SELECT @Start_Date=CAST(DATEADD(DAY,-3,GETDATE()) AS DATE )
	PRINT @Start_Date

	DECLARE @End_Date DATETIME
	SELECT @End_Date=EOMONTH(@Start_Date,0)
	PRINT @End_Date

	--Extracting all the calls made for USAA by Livevox
	IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
	DROP TABLE dbo.#Results
	SELECT EOMONTH([Call_Date],0) EMonth 
		  ,[Call_Date]
		  ,[Session_Id]
		  ,[Agent_Logon_Id]
		  ,[Transaction_Type]
		  ,[Livevox_Result]
		  ,[LV_Client_Name]
		  ,[Account_Number]
		  ,[Call_Duration]
		  ,[Is_RPC]
		  ,[Is_PTP]
		  ,1 as CallCount
		  ,CASE WHEN [Agent_Logon_Id] IS NULL or LEN([Agent_Logon_Id])=0  THEN 0 ELSE 1 END AS Is_Connect
	  INTO dbo.#Results
	  FROM [CLIENT_ANALYTICS].[dbo].[CM_NGIUSAARC]
	  WHERE EOMONTH([Call_Date],0)=@End_Date

	  --SELECT * FROM dbo.#Results ORDER BY CALL_DATE DESC
	order by call_date desc
	--Extracting all the calls made for USAA by CallMiner
	IF OBJECT_ID('tempdb.dbo.#Results_1') IS NOT NULL 
	DROP TABLE dbo.#Results_1
	  SELECT DISTINCT [CALLID]
					 ,REPLACE(REPLACE(REPLACE(REPLACE(RGSSEID,'D.mp3','')
						,'.mp3',''),'N.mp3',''),'_','@') RGSSEID
					 ,[CLNTID]
	  INTO dbo.#Results_1
	  FROM [Speech_ANALYTICS].[dbo].CM_CALLEXPORT
	  WHERE AGNTID in (SELECT DISTINCT [Agent_Logon_Id] 
						FROM dbo.#Results
						WHERE (CASE WHEN [Agent_Logon_Id] IS NULL or 
							LEN([Agent_Logon_Id])=0  THEN 0 ELSE 1 END)=1)
		AND EOM=@End_Date

	--Extracting all the Events made for USAA by CallMiner
	IF OBJECT_ID('tempdb.dbo.#Results_2') IS NOT NULL 
	DROP TABLE dbo.#Results_2
	SELECT DISTINCT EOM
					 ,CALLDT
					 ,CALLID
					 ,CATHIT
					 ,COMPNAME
					 ,STTIME
					 ,ENDTIME
	INTO dbo.#Results_2
	FROM
	(SELECT DISTINCT EOM
					 ,CALLDT
					 ,CALLID
					 ,CATHIT
					 ,COMPNAME
					 ,ROUND(STTIME/1000,0) STTIME
					 ,ROUND(ENDTIME/1000,0) ENDTIME
	  FROM [Speech_ANALYTICS].[dbo].CM_EVNTSAPICMP
	  WHERE [CALLID] in (SELECT DISTINCT [CALLID] FROM dbo.#Results_1)
	  UNION
	  SELECT DISTINCT EOM
					 ,CALLDT
					 ,CALLID
					 ,CATHIT
					 ,COMPNAME
					 ,ROUND(STTIME/1000,0) STTIME
					 ,ROUND(ENDTIME/1000,0) ENDTIME
	  FROM [Speech_ANALYTICS].[dbo].CM_EVNTSAPI
	  WHERE [CALLID] in (SELECT DISTINCT [CALLID] FROM dbo.#Results_1)) A


	  --- Compliance Parameters
	  --MRD

	  --Extracting MRD Hit
	IF OBJECT_ID('tempdb.dbo.#Results_2_1') IS NOT NULL 
	DROP TABLE dbo.#Results_2_1
	SELECT DISTINCT CALLID
				   ,CATHIT
				   ,STTIME 
	INTO dbo.#Results_2_1
	FROM (SELECT CALLID
				,CATHIT
				,STTIME
				,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb 
		  FROM dbo.#Results_2
		  WHERE CATHIT='EN MRD')a
	WHERE rwnumb=1

	--Extracting MM Hit
	IF OBJECT_ID('tempdb.dbo.#Results_2_2') IS NOT NULL 
	DROP TABLE dbo.#Results_2_2
	SELECT DISTINCT CALLID
				   ,CATHIT
				   ,STTIME
	INTO dbo.#Results_2_2
	FROM (SELECT CALLID
				,CATHIT
				,STTIME
				,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb 
	FROM dbo.#Results_2
	WHERE CATHIT='EN AXP Mini Miranda')a
	WHERE rwnumb=1

	--Extracting Creditor Hit
	IF OBJECT_ID('tempdb.dbo.#Results_2_3') IS NOT NULL 
	DROP TABLE dbo.#Results_2_3
	SELECT DISTINCT CALLID
				   ,CATHIT
				   ,STTIME 
	INTO dbo.#Results_2_3
	FROM (SELECT CALLID
				,CATHIT
				,STTIME
				,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb 
	FROM dbo.#Results_2
	WHERE CATHIT='EN AXP Creditor Name')a
	WHERE rwnumb=1

	-- Extracting Balance Hit
	IF OBJECT_ID('tempdb.dbo.#Results_2_4') IS NOT NULL 
	DROP TABLE dbo.#Results_2_4
	SELECT DISTINCT CALLID
				   ,CATHIT
				   ,STTIME 
	INTO dbo.#Results_2_4
	FROM (SELECT CALLID
				,CATHIT
				,STTIME
				,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY STTIME) AS rwnumb 
		FROM dbo.#Results_2
		WHERE CATHIT='EN AXP Balance')a
	WHERE rwnumb=1

	--Combined the FLD parameters based of call ID
	IF OBJECT_ID('tempdb.dbo.#Results_2_5') IS NOT NULL 
	DROP TABLE dbo.#Results_2_5
	SELECT A.CALLID
		  ,1 AS CALLCNT
		  ,CASE WHEN B.CALLID IS NOT NULL THEN 1 ELSE 0 END AS MRDHIT
		  ,B.STTIME AS MRDTIME
		  ,CASE WHEN C.CALLID IS NOT NULL THEN 1 ELSE 0 END AS MMHIT
		  ,C.STTIME AS MMTIME
		  ,CASE WHEN D.CALLID IS NOT NULL THEN 1 ELSE 0 END AS CRDHIT
		  ,D.STTIME AS CRDTTIME
		  ,CASE WHEN E.CALLID IS NOT NULL  THEN 1 ELSE 0 END AS BALHIT
		  ,E.STTIME AS BALTIME
	INTO dbo.#Results_2_5
	FROM dbo.#Results_1 A 
	LEFT JOIN DBO.#Results_2_1 B ON A.CALLID=B.CALLID
	LEFT JOIN DBO.#Results_2_2 c ON A.CALLID=c.CALLID
	LEFT JOIN DBO.#Results_2_3 d ON A.CALLID=d.CALLID
	LEFT JOIN DBO.#Results_2_4 e ON A.CALLID=e.CALLID
	ORDER BY a.CALLID
	---SELECT * FROM dbo.#Results_2_5

	--Getting Second Voice Category for all Calls
	IF OBJECT_ID('tempdb.dbo.#Results_2_6') IS NOT NULL 
	DROP TABLE dbo.#Results_2_6
	SELECT  CALLID
		   ,COMPNAME
		   ,CATHIT
		   ,STTIME
	INTO dbo.#Results_2_6
	FROM(SELECT EOM
		   ,CALLID
		   ,COMPNAME
		   ,CATHIT
		   ,STTIME
		   ,ROW_NUMBER() OVER (PARTITION BY CALLID,CATHIT 
			ORDER BY STTIME) as RNK1
		FROM dbo.#Results_2 
		where CATHIT='EN Second Voice') A
	WHERE RNK1=1

	--Getting Consumer Category for all Calls
	IF OBJECT_ID('tempdb.dbo.#Results_2_7') IS NOT NULL 
	DROP TABLE dbo.#Results_2_7
	SELECT EOM
		  ,CALLID
		  ,COMPNAME
		  ,CATHIT
		  ,STTIME
	INTO dbo.#Results_2_7
	FROM dbo.#Results_2 
		where CATHIT IN ('EN COVID19','EN Income Pitch','EN Employment Pitch'
		,'EN SCRA','EN Fraud Dispute'
	  ,'EN Balance Dispute','EN General Dispute','EN Attorney CS'
	  ,'EN Bankruptcy','EN DNC','EN Deceased','EN Wrong Number CS'
	  ,'EN Controller Consumer','EN Avoider Consumer','EN Pitcher Consumer'
	  ,'EN Promiser Consumer','EN Call Back Request','Em Care'
	  ,'EN Possible Spanish CS','Motive Payment','Motive Previous Call'
	  ,'Motive Mail','Spanish Request','CBR Mentions','IB Misc'
	  ,'Motive Digital Media','EN DCA Mention CS','EN Tax Language CX'
	  ,'EN Denied Info CX','EN CX Refusal Verification','EN CX Payment Information'
	  ,'EN CBR Removal CX','Customer Compliments')


	--Extracting AM Efforts Cat
	IF OBJECT_ID('tempdb.dbo.#Results_2_8') IS NOT NULL 
	DROP TABLE dbo.#Results_2_8
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.AMEFFTTIME
	INTO dbo.#Results_2_8
	FROM (
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.STTIME AS AMEFFTTIME
		  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.STTIME DESC) RNK
	FROM dbo.#Results_2 A 
	WHERE A.CATHIT IN ('EN AM Efforts')) A
	WHERE A.RNK=1

	--Extracting Negotiation CAT
	IF OBJECT_ID('tempdb.dbo.#Results_2_9') IS NOT NULL 
	DROP TABLE dbo.#Results_2_9
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.NEGTIMESTART
	INTO dbo.#Results_2_9
	FROM(SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.STTIME AS NEGTIMESTART
		  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.STTIME) RNK
	FROM dbo.#Results_2 A 
	WHERE A.CATHIT IN ('EN AM Negotiation')) A
	WHERE A.RNK=1

	IF OBJECT_ID('tempdb.dbo.#Results_2_9_1') IS NOT NULL 
	DROP TABLE dbo.#Results_2_9_1
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.NEGTIMEEND
	INTO dbo.#Results_2_9_1
	FROM(SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.STTIME AS NEGTIMEEND
		  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.STTIME DESC) RNK
	FROM dbo.#Results_2 A 
	WHERE A.CATHIT IN ('EN AM Negotiation')) A
	WHERE A.RNK=1

	--Extracting Open and close ended Question 
	IF OBJECT_ID('tempdb.dbo.#Results_2_10') IS NOT NULL 
	DROP TABLE dbo.#Results_2_10
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.STTIME AS QUESTIME
	INTO dbo.#Results_2_10
	FROM dbo.#Results_2 A 
	WHERE A.CATHIT IN ('EN AM Open Questions','EN AM Close Questions')

	--Extracting Verification CAT
	IF OBJECT_ID('tempdb.dbo.#Results_2_11') IS NOT NULL 
	DROP TABLE dbo.#Results_2_11
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.VERTIME
	INTO dbo.#Results_2_11
	FROM(SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.STTIME AS VERTIME
		  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.STTIME) RNK
	FROM dbo.#Results_2 A 
	WHERE A.CATHIT IN ('EN AM Verification')) A
	WHERE A.RNK=1

	--Extracting Dissatisfaction CAT
	IF OBJECT_ID('tempdb.dbo.#Results_2_12') IS NOT NULL 
	DROP TABLE dbo.#Results_2_12
	SELECT A.CALLID
		  ,A.COMPNAME
		  ,A.CATHIT
		  ,A.STTIME AS QUESTIME
	INTO dbo.#Results_2_12
	FROM dbo.#Results_2 A 
	WHERE A.CATHIT IN ('EN CX Dissatisfaction')


	--Combining FLD with Second Voice 
	IF OBJECT_ID('tempdb.dbo.#Results_2_13') IS NOT NULL 
	DROP TABLE dbo.#Results_2_13
	SELECT A.CALLID
		  ,A.CALLCNT
		  ,A.MRDHIT
		  ,A.MRDTIME	
		  ,A.MMHIT	
		  ,A.MMTIME
		  ,A.CRDHIT
		  ,A.CRDTTIME
		  ,A.BALHIT
		  ,A.BALTIME
		  ,CASE WHEN A.BALTIME IS NULL AND A.CRDTTIME IS NULL THEN 0
			WHEN A.BALTIME IS NOT NULL THEN A.BALTIME
			WHEN A.BALTIME IS NULL AND A.CRDTTIME IS NOT NULL THEN A.CRDTTIME
			END CONSLANGTIME
		  ,B.CATHIT as SNDVCATHIT
		  ,B.COMPNAME as SNDVCOMPNAME
		  ,B.STTIME as SNDVSTTIME
		  ,CASE WHEN B.STTIME IS NOT NULL THEN 1 ELSE 0 END AS SNDVFLAG
	INTO dbo.#Results_2_13
	FROM dbo.#Results_2_5 a
		LEFT JOIN dbo.#Results_2_6 b 
			on a.CALLID=b.CALLID

	--SELECT * FROM #Results_2_13

	--extracting first CXlanguage Based of opening where applicable 
	IF OBJECT_ID('tempdb.dbo.#Results_2_14') IS NOT NULL 
	DROP TABLE dbo.#Results_2_14
	SELECT H.CALLID
		  ,H.COMPNAME
		  ,H.CATHIT
		  ,H.CXLANTIME
	INTO dbo.#Results_2_14
	FROM(
	SELECT G.CALLID
		  ,G.COMPNAME
		  ,G.CATHIT
		  ,G.CXLANTIME
		  ,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY CXLANTIME) RNK
	FROM
	(
	SELECT D.CALLID
		  ,D.COMPNAME
		  ,D.CATHIT
		  ,D.STTIME AS CXLANTIME
		  ,E.CONSLANGTIME
		  ,CASE WHEN E.CONSLANGTIME>D.STTIME THEN 'LNGFLAGA' ELSE 'LNGFLAGB' END LNGFLAGS
		  ,F.Check1
	FROM dbo.#Results_2_7 D
		LEFT JOIN dbo.#Results_2_13 E
			ON D.CALLID=E.CALLID
		LEFT JOIN (
	SELECT C.CALLID
		  ,CASE WHEN SUM(C.LNGFLAGB)=0 THEN 'LNGFLAGA' ELSE 'LNGFLAGB' END as Check1
	FROM(
	SELECT A.CALLID
		  ,CASE WHEN B.CONSLANGTIME>A.STTIME THEN 1 ELSE 0 END LNGFLAGA
		  ,CASE WHEN B.CONSLANGTIME<=A.STTIME THEN 1 ELSE 0 END LNGFLAGB
	FROM dbo.#Results_2_7 A
		LEFT JOIN dbo.#Results_2_13 B
			ON A.CALLID=B.CALLID) C
	GROUP BY C.CALLID) F
		on D.CALLID=F.CALLID) G
		WHERE G.LNGFLAGS=G.Check1) H
		WHERE H.RNK=1

	--Select * from dbo.#Results_2_14

	--Getting COUNT of open and close question
	IF OBJECT_ID('tempdb.dbo.#Results_2_15') IS NOT NULL 
	DROP TABLE dbo.#Results_2_15
	SELECT H.CALLID
		  ,SUM(CASE WHEN H.CATHIT='EN AM Open Questions' THEN 1 else 0 end) as OPNQUEST
		  ,SUM(CASE WHEN H.CATHIT='EN AM Close Questions' THEN 1 else 0 end) as CLOQUEST 
	INTO dbo.#Results_2_15
	FROM
	(
	SELECT D.CALLID
		  ,D.COMPNAME
		  ,D.CATHIT
	FROM dbo.#Results_2_10 D
		LEFT JOIN dbo.#Results_2_13 E
			ON D.CALLID=E.CALLID
	WHERE E.CONSLANGTIME<=D.QUESTIME) H
	GROUP BY  H.CALLID

	--Combining FLC-SV Nego AM efforts open-close ques
	IF OBJECT_ID('tempdb.dbo.#Results_2_16') IS NOT NULL 
	DROP TABLE dbo.#Results_2_16
	SELECT A.CALLID
		  ,A.CALLCNT
		  ,A.MRDHIT	
		  ,A.MRDTIME
		  ,A.MMHIT	
		  ,A.MMTIME
		  ,A.CRDHIT
		  ,A.CRDTTIME
		  ,A.BALHIT
		  ,A.BALTIME
		  ,A.SNDVCATHIT
		  ,A.SNDVCOMPNAME
		  ,A.SNDVSTTIME
		  ,A.SNDVFLAG
		  ,B.COMPNAME AS CXLANGCOMPNAME
		  ,B.CATHIT AS CXLANGCATHIT
		  ,B.CXLANTIME
		  ,C.COMPNAME AS NEGOSTARTCOMPNAME
		  ,C.NEGTIMESTART
		  ,CASE WHEN C.CALLID IS NOT NULL THEN 1 ELSE 0 END NEGOFLAG
		  ,D.COMPNAME AS NEGOENDCOMPNAME
		  ,D.NEGTIMEEND
		  ,E.COMPNAME AS AMEFFORTCOMPNAME
		  ,E.AMEFFTTIME
		  ,CASE WHEN E.CALLID IS NOT NULL THEN 1 ELSE 0 END AMEFFORTFLAG
		  ,F.OPNQUEST
		  ,F.CLOQUEST
		  ,CASE WHEN G.CALLID IS NOT NULL THEN 1 ELSE 0 END VERFLAG
		  ,G.VERTIME
	INTO dbo.#Results_2_16
	FROM dbo.#Results_2_13 A
		LEFT JOIN dbo.#Results_2_14 B
			ON A.CALLID=B.CALLID
		LEFT JOIN dbo.#Results_2_9 C
			ON A.CALLID=C.CALLID
		LEFT JOIN dbo.#Results_2_9_1 D
			ON A.CALLID=D.CALLID
		LEFT JOIN dbo.#Results_2_8 E
			ON A.CALLID=E.CALLID
		LEFT JOIN dbo.#Results_2_15 F
			ON A.CALLID=F.CALLID
		LEFT JOIN dbo.#Results_2_11 G
			ON A.CALLID=G.CALLID

	--SELECT * FROM #Results_2_16

	--CALCULATING DISSAT SCORE
	IF OBJECT_ID('tempdb.dbo.#Results_2_17') IS NOT NULL
	DROP TABLE dbo.#Results_2_17
	SELECT CALLID
			,COUNT(CASE WHEN COMPNAME LIKE 'H%' THEN 1 END) AS H_Count
			,COUNT(CASE WHEN COMPNAME LIKE 'M%' THEN 1 END) AS M_Count
			,COUNT(CASE WHEN COMPNAME LIKE 'L%' THEN 1 END) AS L_Count
			,CASE WHEN COUNT(CASE WHEN COMPNAME LIKE 'H%' THEN 1 END) > 0 THEN 0
				  WHEN COUNT(CASE WHEN COMPNAME LIKE 'M%' THEN 1 END) > 0 THEN 0.5
				  WHEN COUNT(CASE WHEN COMPNAME LIKE 'L%' THEN 1 END) > 0 THEN 0.75
			 ELSE 0 END AS DISSATSCR
	INTO dbo.#Results_2_17
	FROM dbo.#Results_2_12
	GROUP BY CALLID

	IF OBJECT_ID('tempdb.dbo.#Results_2_17_1') IS NOT NULL
	DROP TABLE dbo.#Results_2_17_1
	SELECT CALLID
		  ,COMPNAME
		  ,QUESTIME
	INTO dbo.#Results_2_17_1
	FROM
	(
	SELECT CALLID
		  ,COMPNAME
		  ,QUESTIME
		  ,ROW_NUMBER() OVER (PARTITION BY CALLID ORDER BY SEVRITYFLAG,QUESTIME) RNK
	FROM
	(
	SELECT CALLID
		  ,COMPNAME
		  ,QUESTIME
		  ,CASE WHEN COMPNAME LIKE 'H%' THEN 1 
				WHEN COMPNAME LIKE 'M%' THEN 2 
				ELSE 3 END SEVRITYFLAG 
	FROM dbo.#Results_2_12) A) B
	WHERE B.RNK=1

	IF OBJECT_ID('tempdb.dbo.#Results_2_18') IS NOT NULL
	DROP TABLE dbo.#Results_2_18
	SELECT A.CALLID
			,A.CALLCNT
			,A.MRDHIT
			,A.MRDTIME
			,A.MMHIT
			,A.MMTIME
			,A.CRDHIT
			,A.CRDTTIME
			,A.BALHIT
			,A.BALTIME
			,A.SNDVCATHIT
			,A.SNDVCOMPNAME
			,A.SNDVSTTIME
			,A.SNDVFLAG
			,A.CXLANGCOMPNAME
			,A.CXLANGCATHIT
			,A.CXLANTIME
			,A.NEGOSTARTCOMPNAME
			,A.NEGTIMESTART
			,A.NEGOFLAG
			,A.NEGOENDCOMPNAME
			,A.NEGTIMEEND
			,A.AMEFFORTCOMPNAME
			,A.AMEFFTTIME
			,A.AMEFFORTFLAG
			,A.OPNQUEST
			,A.CLOQUEST
			,A.VERFLAG
			,A.VERTIME
			,ISNULL(B.H_Count,0) H_Count
			,ISNULL(B.M_Count,0) M_Count
			,ISNULL(B.L_Count,0) L_Count
			,ISNULL(B.DISSATSCR,1) DISSATSCR
	 INTO dbo.#Results_2_18
	 from dbo.#Results_2_16 A
		LEFT JOIN dbo.#Results_2_17 B
			ON A.CALLID=b.CALLID

			--SELECT * FROM #Results_2_18

	--Calculating OPP, EARLYHANGUP, Speech App, processed, TTOPENG, TTNEG and NULL 
	IF OBJECT_ID('tempdb.dbo.#Results_2_19') IS NOT NULL
	DROP TABLE dbo.#Results_2_19
	SELECT A.EMonth
			,A.Call_Date
			,A.Session_Id
			,A.Agent_Logon_Id
			,A.Transaction_Type
			,A.Livevox_Result
			,A.LV_Client_Name
			,A.Account_Number
			,A.Call_Duration
			,ISNULL(A.Is_RPC,0) Is_RPC
			,ISNULL(A.Is_PTP,0) Is_PTP
			,A.CallCount
			,A.Is_Connect
			,CASE WHEN A.Call_Duration>60 THEN 1 ELSE 0 END AS DISSATOPP
			,CASE WHEN a.Is_Connect=1 AND a.[Call_Duration]>=30  
				THEN 1 ELSE 0 END AS SAMINEAPP
  			,CASE WHEN a.Is_Connect=1 AND a.[Call_Duration]>=30 
				AND B.CALLID IS NOT NULL THEN 1 ELSE 0 END AS SAMINECOMP
			,CASE WHEN a.Is_Connect=1 AND a.[Call_Duration]<60 
				THEN 1 ELSE 0 END AS EARLYHANGUP
			,CASE WHEN A.Call_Duration>240 AND A.Is_RPC=1 
				AND A.Transaction_type='INBOUND'  AND  B.CALLID IS NOT NULL THEN 1 ELSE 0 END AS VEROPP
			,B.CALLID
			,C.MRDHIT
			,C.MRDTIME
			,C.MMHIT
			,C.MMTIME
			,C.CRDHIT
			,C.CRDTTIME
			,C.BALHIT
			,C.BALTIME
			,C.SNDVCATHIT
			,C.SNDVCOMPNAME
			,C.SNDVSTTIME
			,ISNULL(C.SNDVFLAG,0) SNDVFLAG
			,C.CXLANGCOMPNAME
			,C.CXLANGCATHIT
			,C.CXLANTIME
			,C.NEGOSTARTCOMPNAME
			,C.NEGTIMESTART
			,C.NEGOFLAG
			,C.NEGOENDCOMPNAME
			,C.NEGTIMEEND
			,C.AMEFFORTCOMPNAME
			,C.AMEFFTTIME
			,C.AMEFFORTFLAG
			,C.OPNQUEST
			,C.CLOQUEST
			,CASE WHEN A.Call_Duration>240 AND A.Is_RPC=1 
				AND A.Transaction_type='INBOUND' AND  B.CALLID IS NOT NULL
					 THEN ISNULL(C.VERFLAG,0) ELSE 0 END VERFLAG
			,CASE WHEN A.Call_Duration>240 AND A.Is_RPC=1 
				AND A.Transaction_type='INBOUND' AND  B.CALLID IS NOT NULL
					 THEN C.VERTIME ELSE 0 END VERTIME
			,C.H_Count
			,C.M_Count
			,C.L_Count
			,C.DISSATSCR
			,CASE WHEN B.CALLID IS NOT NULL THEN C.NEGTIMEEND-C.NEGTIMESTART END AS TTNEG
			,CASE WHEN B.CALLID IS NOT NULL THEN C.NEGTIMESTART-E.CONSLANGTIME END AS TTINPROB
			,D.DIS1CAT
			,E.CONSLANGTIME AS TTOPENG
			,CASE WHEN B.CALLID IS NOT NULL AND (D.DIS1CAT='TPC' OR  A.Is_RPC=1) THEN 1 ELSE 0 END AS MRDOPP
			,CASE WHEN B.CALLID IS NOT NULL AND A.Call_Duration>60 AND A.Is_RPC=1 
				THEN 1 ELSE 0 END AS MMOPP
			,CASE WHEN B.CALLID IS NOT NULL AND A.Call_Duration>90 AND A.Is_RPC=1 
				THEN 1 ELSE 0 END AS CRDOPP
			,CASE WHEN B.CALLID IS NOT NULL AND A.Call_Duration>120 AND A.Is_RPC=1 
				THEN 1 ELSE 0 END AS BALOPP
			,CASE WHEN B.CALLID IS NOT NULL AND A.Call_Duration>240 AND A.Is_RPC=1 
				THEN 1 ELSE 0 END AS NEGOPP
	INTO dbo.#Results_2_19
	FROM dbo.#Results A
		LEFT JOIN dbo.#Results_1 B
			ON A.Session_Id=B.RGSSEID
		LEFT JOIN dbo.#Results_2_18 C
			ON B.CALLID=C.CALLID
		LEFT JOIN [Speech_Analytics].[dbo].[CM_LVCDE] D
			ON A.LV_Client_Name=D.RECDP 
				AND A.Livevox_Result=D.DIS1
		LEFT JOIN dbo.#Results_2_13 E --CONSLANGTIME
			ON B.CALLID=E.CALLID

	IF OBJECT_ID('tempdb.dbo.#Results_2_20') IS NOT NULL
	DROP TABLE dbo.#Results_2_20
	SELECT A.EMonth
			,A.Call_Date
			,A.Session_Id
			,A.Agent_Logon_Id
			,A.Transaction_Type
			,A.Livevox_Result
			,A.LV_Client_Name
			,A.Account_Number
			,A.Call_Duration
			,A.Is_RPC
			,A.Is_PTP
			,A.CallCount
			,A.Is_Connect
			,A.DISSATOPP
			,A.SAMINEAPP
			,A.SAMINECOMP
			,A.EARLYHANGUP
			,A.VEROPP
			,A.CALLID
			,CASE WHEN A.MRDHIT=1 AND A.MRDOPP=0 THEN 0 ELSE A.MRDHIT END AS MRDHIT
			,A.MRDTIME
			,CASE WHEN A.MMHIT=1 AND A.MMOPP=0 THEN 0 ELSE A.MMHIT END MMHIT
			,A.MMTIME
			,CASE WHEN A.CRDHIT=1 AND A.CRDOPP=0 THEN 0 ELSE A.CRDHIT END CRDHIT
			,A.CRDTTIME
			,CASE WHEN A.BALHIT=1 AND A.BALOPP=0 THEN 0 ELSE A.BALHIT END BALHIT
			,A.BALTIME
			,A.SNDVCATHIT
			,A.SNDVCOMPNAME
			,A.SNDVSTTIME
			,A.SNDVFLAG
			,A.CXLANGCOMPNAME
			,CASE WHEN A.CXLANGCOMPNAME IS NOT NULL THEN B.CATHITCATG ELSE NULL END AS CXLANGCATG
			,A.CXLANGCATHIT
			,A.CXLANTIME
			,A.NEGOSTARTCOMPNAME
			,A.NEGTIMESTART
			,CASE WHEN A.NEGOFLAG=1 AND A.NEGOPP=0 THEN 0 ELSE A.NEGOFLAG END NEGOFLAG
			,A.NEGOENDCOMPNAME
			,A.NEGTIMEEND
			,A.AMEFFORTCOMPNAME
			,A.AMEFFTTIME
			,A.AMEFFORTFLAG
			,A.OPNQUEST
			,A.CLOQUEST
			,CASE WHEN A.VERFLAG=1 AND A.VEROPP=0 THEN 0 ELSE A.VERFLAG END VERFLAG
			,A.VERTIME
			,A.H_Count
			,A.M_Count
			,A.L_Count
			,A.DISSATSCR
			,A.TTNEG
			,A.TTINPROB
			,A.DIS1CAT
			,A.TTOPENG
			,A.MRDOPP
			,A.MMOPP
			,A.CRDOPP
			,A.BALOPP
			,A.NEGOPP
			,C.COMPNAME DISCOMPNAME
			,C.QUESTIME DISCOPTIME
	INTO dbo.#Results_2_20
	FROM dbo.#Results_2_19 A
	LEFT JOIN (SELECT DISTINCT COMPNAME,CATHITCATG 
				FROM [Speech_Analytics].[dbo].[CM_CXCATGRP]) B 
	ON A.CXLANGCOMPNAME = B.COMPNAME
	LEFT JOIN dbo.#Results_2_17_1 C
			ON A.CALLID=C.CALLID

	DELETE FROM Speech_Analytics.dbo.CM_Rpt_USAACMP
	WHERE EMonth=@End_Date

	INSERT INTO Speech_Analytics.dbo.CM_Rpt_USAACMP
	SELECT   EMonth
			,Call_Date
			,Session_Id
			,Agent_Logon_Id
			,Transaction_Type
			,Livevox_Result
			,LV_Client_Name
			,Account_Number
			,Call_Duration
			,Is_RPC
			,Is_PTP
			,CallCount
			,Is_Connect
			,DISSATOPP
			,SAMINEAPP
			,SAMINECOMP
			,EARLYHANGUP
			,VEROPP
			,CALLID
			,MRDHIT
			,MRDTIME
			,MMHIT
			,MMTIME
			,CRDHIT
			,CRDTTIME
			,BALHIT
			,BALTIME
			,SNDVCATHIT
			,SNDVCOMPNAME
			,SNDVSTTIME
			,SNDVFLAG
			,CXLANGCOMPNAME
			,CXLANGCATG
			,CXLANGCATHIT
			,CXLANTIME
			,NEGOSTARTCOMPNAME
			,NEGTIMESTART
			,NEGOFLAG
			,NEGOENDCOMPNAME
			,NEGTIMEEND
			,AMEFFORTCOMPNAME
			,AMEFFTTIME
			,AMEFFORTFLAG
			,OPNQUEST
			,CLOQUEST
			,VERFLAG
			,VERTIME
			,H_Count
			,M_Count
			,L_Count
			,DISSATSCR
			,TTNEG
			,TTINPROB
			,DIS1CAT
			,TTOPENG
			,MRDOPP
			,MMOPP
			,CRDOPP
			,BALOPP
			,NEGOPP
			,DISCOMPNAME
			,DISCOPTIME
	FROM dbo.#Results_2_20


	DECLARE @tab char(1) = CHAR(9)

	DECLARE @totcntmm VARCHAR (MAX);
	SELECT @totcntmm=CAST(COUNT(Session_Id) AS varchar) FROM dbo.#Results_2_20
	PRINT @totcntmm

	DECLARE @body1 VARCHAR (MAX); 
			SET @body1 = 'Hi All,
		
	Daily USAA Speech SQL code executed for '+convert(varchar,@End_Date,102)+'

	Total count of rows added are '+ (@totcntmm) +'. 

	Regards,
	Business Analytics';
			print @body1
	DECLARE @subject1 VARCHAR (MAX); 
			SET @subject1 = 'Daily USAA Speech SQL code executed for ' + convert(varchar,@End_Date,102);
			print @subject1
		--send email
		if (SELECT count(Session_Id) FROM dbo.#Results_2_20)>0
			EXEC msdb.dbo.sp_send_dbmail
			@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
			@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
			@recipients = 'dw@radiusgs.com;business.analytics@radiusgs.com',
			@copy_recipients='Pulkit.Jain@radiusgs.com;',
			@subject = @subject1,
			@body = @body1;

END