
create procedure [dbo].[usp_68_SQL_AXPLEGAL_LTO]
as
	begin
--Setting a end of moth for automatic run
DECLARE @End_Month DATETIME;

SET @End_Month=EOMONTH(CAST(DATEADD(DAY,-3,GETDATE()) AS DATE ),0)
PRINT @End_Month

--get all calls for current month based of client ids for Legal
IF OBJECT_ID('tempdb.dbo.#Results') IS NOT NULL 
DROP TABLE dbo.#Results
SELECT DISTINCT A.EOM
         ,A.AGNTID
         ,A.CALLID
         ,A.CALLDT
         ,A.RGSSEID 
         ,A.DIS1
         ,A.RECDP
         ,A.PHNUMB
         ,A.CLNTID
         ,A.SILT
         ,A.CLDUR
		 ,A.RGSACC
		 ,A.DIR
         ,B.DIS1CAT 
INTO dbo.#Results
FROM [DBO].[CM_CALLEXPORT] A LEFT JOIN
       [DBO].[CM_LVCDE] B 
              ON A.RECDP=B.RECDP AND A.DIS1=B.DIS1
		LEFT JOIN [Speech_Analytics].[dbo].[CM_DIMAXPRcodes] C 
		ON A.CLNTID= C.CLINTID
WHERE C.SA_Category='AXP Legal'
	AND A.EOM=@End_Month 
    AND ([SMPLFLG] IS NULL OR [SMPLFLG]=0)
    AND b.DIS1CAT IN ('RPC','PTP')
ORDER BY A.CALLDT



--Getting Second Voice, DownPayment and Second Voice Category for all Calls
IF OBJECT_ID('tempdb.dbo.#Results1') IS NOT NULL 
DROP TABLE dbo.#Results1
SELECT EOM
          ,CALLID
		  ,CALLDT
		  ,CATHIT
          ,COMPNAME
          ,ROUND(STTIME/1000,0) STTIME
          ,ROW_NUMBER() OVER (PARTITION BY CALLID,CATHIT 
              ORDER BY STTIME) as RNK1
  INTO dbo.#Results1
  FROM [dbo].[CM_EVNTSAPI] 
  where EOM=@End_Month
        AND CATHIT IN ('EN_Legal Talk Off', 'EN Second Voice', 'EN Down Payment language')
		AND CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results)

--Getting Behaviour Category for CAT HIT
IF OBJECT_ID('tempdb.dbo.#Results2') IS NOT NULL 
DROP TABLE dbo.#Results2
SELECT EOM
	  ,CALLID
	  ,COMPNAME
	  ,CATHIT
	  ,ROUND(STTIME/1000,0) STTIME
	  ,ROW_NUMBER() OVER (PARTITION BY CALLID,CATHIT 
		ORDER BY STTIME) as RNK1
  INTO dbo.#Results2
  FROM [dbo].[CM_EVNTSAPI] 
  where EOM=@End_Month
		AND CATHIT IN ('EN COVID19','EN Income Pitch','EN Employment Pitch'
  ,'EN Consumer Misunderstanding','EN SCRA','EN Fraud Dispute'
  ,'EN Balance Dispute','EN General Dispute','EN Attorney CS'
  ,'EN Bankruptcy','EN DNC','EN Deceased','EN Wrong Number CS'
  ,'EN Controller Consumer','EN Avoider Consumer','EN Pitcher Consumer'
  ,'EN Promiser Consumer','EN Call Back Request','Em Care'
  ,'EN Possible Spanish CS','Motive Payment','Motive Previous Call'
  ,'Motive Mail','Spanish Request','CBR Mentions','IB Misc'
  ,'Motive Digital Media','EN DCA Mention CS','EN Tax Language CX'
  ,'EN Denied Info CX','EN CX Refusal Verification','EN CX Payment Information'
  ,'EN CBR Removal CX','Compliments','Customer Compliments'
  ,'EN AXP Creditor Name','EN AXP Balance', 'EN AM Negotiation')
  AND CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results)

--extarcting the category group
IF OBJECT_ID('tempdb.dbo.#Results3') IS NOT NULL 
DROP TABLE dbo.#Results3
SELECT DISTINCT [CATHIT]
	  ,[COMPNAME]
	  ,[CATHITCATG]
INTO dbo.#Results3
FROM [dbo].[CM_CXCATGRP]

-- EXTRACTING OPENING time 
IF OBJECT_ID('tempdb.dbo.#Results4') IS NOT NULL 
DROP TABLE dbo.#Results4
SELECT CALLID
	  ,OPNTIME
INTO dbo.#Results4
FROM (SELECT CALLID
	  ,STTIME AS OPNTIME
	  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
		ORDER BY STTIME DESC) as RNK1
FROM dbo.#Results2
WHERE CATHIT IN ('EN AXP Creditor Name','EN AXP Balance')
	  AND STTIME<=180) a
WHERE a.RNK1=1

 -- Calculating & Extarcting Unique Down Payment language
IF OBJECT_ID('tempdb.dbo.#Results5') IS NOT NULL 
DROP TABLE dbo.#Results5
SELECT CALLID
	  ,STTIME
	  ,CATHIT
	  ,COMPNAME
INTO dbo.#Results5
FROM (SELECT CALLID
	  ,STTIME
	  ,CATHIT
	  ,COMPNAME
	  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
	  ORDER BY STTIME DESC) AS RNK1
	  FROM dbo.#Results1 WHERE CATHIT='EN Down Payment language') a
WHERE a.RNK1=1 

-- Calculating & Extarcting Unique Second Voice
IF OBJECT_ID('tempdb.dbo.#Results6') IS NOT NULL 
DROP TABLE dbo.#Results6
SELECT CALLID
	  ,STTIME
	  ,CATHIT
	  ,COMPNAME
INTO dbo.#Results6
FROM (SELECT CALLID
	  ,STTIME
	  ,CATHIT
	  ,COMPNAME
	  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
	  ORDER BY STTIME DESC) AS RNK1
	  FROM dbo.#Results1 WHERE CATHIT='EN Second Voice') a
WHERE a.RNK1=1

-- Calculating & Extarcting Unique Legal Talk off
IF OBJECT_ID('tempdb.dbo.#Results7') IS NOT NULL 
DROP TABLE dbo.#Results7
SELECT CALLID
	  ,STTIME
	  ,CATHIT
	  ,COMPNAME
INTO dbo.#Results7
FROM (SELECT CALLID
	  ,STTIME
	  ,CATHIT
	  ,COMPNAME
	  ,ROW_NUMBER() OVER (PARTITION BY CALLID 
	  ORDER BY STTIME DESC) AS RNK1
	  FROM dbo.#Results1 WHERE CATHIT='EN_Legal Talk Off') a
WHERE a.RNK1=1

-- Calculating Negotiation StTime
IF OBJECT_ID('tempdb.dbo.#Results8') IS NOT NULL 
DROP TABLE dbo.#Results8
SELECT CALLID
	  ,NEGSTTIME
INTO dbo.#Results8
FROM (SELECT a.CALLID
	  ,a.STTIME AS NEGSTTIME
	  ,ROW_NUMBER() OVER (PARTITION BY a.CALLID 
		ORDER BY a.STTIME) as RNK1
FROM dbo.#Results2 a
	LEFT JOIN dbo.#Results4 b
		on a.CALLID=b.CALLID
WHERE a.CATHIT IN ('EN AM Negotiation')
	  AND a.STTIME>b.OPNTIME) c
WHERE c.RNK1=1

--Negotiation End Time
IF OBJECT_ID('tempdb.dbo.#Results8_1') IS NOT NULL 
DROP TABLE dbo.#Results8_1
SELECT CALLID
	  ,NEGENDTIME
INTO dbo.#Results8_1 
FROM (SELECT a.CALLID
	  ,a.STTIME AS NEGENDTIME
	  ,ROW_NUMBER() OVER (PARTITION BY a.CALLID 
		ORDER BY a.STTIME DESC) as RNK1
FROM dbo.#Results2 a
WHERE a.CATHIT IN ('EN AM Negotiation')) c
WHERE c.RNK1=1

--Negotiation Start & End Time combined
IF OBJECT_ID('tempdb.dbo.#Results8_2') IS NOT NULL 
DROP TABLE dbo.#Results8_2
SELECT A.CALLID
	  ,A.NEGSTTIME
	  ,CASE WHEN A.NEGSTTIME=B.NEGENDTIME 
		THEN 0 ELSE B.NEGENDTIME END NEGENDTIME
INTO dbo.#Results8_2
FROM dbo.#Results8 A LEFT JOIN
	dbo.#Results8_1 B 
		on a.CALLID=b.CALLID
WHERE B.CALLID IS NOT NULL

-- extarcting consumer words CALLS
IF OBJECT_ID('tempdb.dbo.#Results9') IS NOT NULL 
DROP TABLE dbo.#Results9
SELECT A.CALLID
	,A.CATHIT
	,A.COMPNAME
	,A.STTIME AS CXLANGTIME
INTO dbo.#Results9
FROM dbo.#Results2 A 
WHERE A.CATHIT IN ('EN COVID19','EN Income Pitch','EN Employment Pitch'
,'EN Consumer Misunderstanding','EN SCRA','EN Fraud Dispute','EN Balance Dispute'
,'EN General Dispute','EN Attorney CS','EN Bankruptcy','EN DNC','EN Deceased'
,'EN Wrong Number CS','EN Controller Consumer','EN Avoider Consumer'
,'EN Pitcher Consumer','EN Promiser Consumer','EN Call Back Request','Em Care'
,'EN Possible Spanish CS','Motive Payment','Motive Previous Call'
,'Motive Mail','Spanish Request','CBR Mentions','IB Misc','Motive Digital Media'
,'EN DCA Mention CS','EN Tax Language CX','EN Denied Info CX'
,'EN CX Refusal Verification','EN CX Payment Information','EN CBR Removal CX'
,'Compliments','Customer Compliments')

--Combining the Down Payment , Second Voice & 
--Legal Talk off hits for further calculations
IF OBJECT_ID('tempdb.dbo.#Results_1') IS NOT NULL 
DROP TABLE dbo.#Results_1
select a.CALLID
	 ,CASE WHEN B.CALLID IS NOT NULL THEN 1 ELSE 0 END AS DPHIT
	 ,CASE WHEN B.CALLID IS NOT NULL THEN B.STTIME ELSE 0 END AS DPSTTIME
	 ,CASE WHEN C.CALLID IS NOT NULL THEN 1 ELSE 0 END AS SVCHIT
	 ,CASE WHEN C.CALLID IS NOT NULL THEN C.STTIME ELSE 0 END AS SVCSTTIME
	 ,CASE WHEN D.CALLID IS NOT NULL THEN 1 ELSE 0 END AS LTOHIT
	 ,CASE WHEN D.CALLID IS NOT NULL THEN D.STTIME ELSE 0 END AS LTOSTTIME
	 ,CASE WHEN D.CALLID IS NOT NULL THEN D.COMPNAME END AS LTOCOMPNME
 INTO dbo.#Results_1
 from #Results a 
	left join #Results5 b 
		on a.CALLID=b.CALLID
	left join #Results6 c 
		on a.CALLID=c.CALLID
	left join #Results7 d 
		on a.CALLID=d.CALLID

-- Consumer Language WHERE No Hits CALLS
IF OBJECT_ID('tempdb.dbo.#Results_2') IS NOT NULL 
DROP TABLE dbo.#Results_2
SELECT CALLID,
		CATHIT,
		COMPNAME,
		CXLANGTIME
INTO dbo.#Results_2
FROM
(SELECT a.*
	  , ROW_NUMBER() OVER (PARTITION BY a.CALLID 
		ORDER BY a.CXLANGTIME DESC) as RNK1 
FROM dbo.#Results9 a LEFT JOIN
		dbo.#Results4 B 
			on a.CALLID=b.CALLID
WHERE a.CALLID IN (SELECT DISTINCT CALLID FROM dbo.#Results_1 WHERE DPHIT+SVCHIT+LTOHIT=0)
	AND a.CXLANGTIME>=b.OPNTIME) c
	WHERE C.RNK1=1


-- Calculating Befor & After Consumer 
--Language for Legal Talk off
IF OBJECT_ID('tempdb.dbo.#Results_3') IS NOT NULL 
DROP TABLE dbo.#Results_3
SELECT A.CALLID
	  ,A.CATHIT
	  ,A.COMPNAME
	  ,A.CXLANGTIME
	  ,B.LTOSTTIME
	  ,CASE WHEN B.LTOSTTIME<=A.CXLANGTIME THEN A.COMPNAME END CXCOMPAFTERLTO
	  ,CASE WHEN B.LTOSTTIME<=A.CXLANGTIME THEN A.CXLANGTIME END CXSTTIMEAFTERLTO
	  ,CASE WHEN B.LTOSTTIME>A.CXLANGTIME THEN A.COMPNAME END CXCOMPBEFLTO
	  ,CASE WHEN B.LTOSTTIME>=A.CXLANGTIME THEN A.CXLANGTIME END CXSTTIMEBEFLTO
INTO dbo.#Results_3
FROM dbo.#Results9 a LEFT JOIN
		dbo.#Results_1 B 
			on a.CALLID=b.CALLID
WHERE B.LTOHIT=1

-- Calculating First instance of   Consumer 
--Language After Legal Talk off
IF OBJECT_ID('tempdb.dbo.#Results_3_1') IS NOT NULL 
DROP TABLE dbo.#Results_3_1
SELECT B.CALLID
	  ,B.CATHIT AS CXCATHITAFTERLTO
	  ,B.CXCOMPAFTERLTO
	  ,B.CXSTTIMEAFTERLTO
INTO dbo.#Results_3_1
FROM (
SELECT A.CALLID
	  ,A.CATHIT
	  ,A.CXCOMPAFTERLTO
	  ,A.CXSTTIMEAFTERLTO
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.CXSTTIMEAFTERLTO) RNK1
FROM dbo.#Results_3 A
WHERE A.CXCOMPAFTERLTO IS NOT NULL) B
WHERE B.RNK1=1

-- Calculating First instance of   Consumer 
--Language Before Legal Talk off
IF OBJECT_ID('tempdb.dbo.#Results_3_2') IS NOT NULL 
DROP TABLE dbo.#Results_3_2
SELECT B.CALLID
	  ,B.CXCATHITBEFLTO
	  ,B.CXCOMPBEFLTO
	  ,B.CXSTTIMEBEFLTO
INTO dbo.#Results_3_2
FROM
(SELECT A.CALLID
	  ,A.CATHIT AS CXCATHITBEFLTO
	  ,A.COMPNAME
	  ,A.CXCOMPBEFLTO
	  ,A.CXSTTIMEBEFLTO
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.CXSTTIMEBEFLTO) RNK1
FROM dbo.#Results_3 A
WHERE A.CXSTTIMEBEFLTO IS NOT NULL) B
WHERE RNK1=1

-- Calculating First instance of   Consumer 
--Language with No Legal Talk off
IF OBJECT_ID('tempdb.dbo.#Results_4') IS NOT NULL 
DROP TABLE dbo.#Results_4
SELECT D.CALLID
	  ,D.CXCATHITNONLTO
	  ,D.CXCOMPNONLTO
	  ,D.CXSTTIMENONLTO
INTO dbo.#Results_4
FROM
(
SELECT A.CALLID
	  ,A.CATHIT CXCATHITNONLTO
	  ,A.COMPNAME CXCOMPNONLTO
	  ,A.CXLANGTIME CXSTTIMENONLTO
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.CXLANGTIME) RNK1
FROM dbo.#Results9 a LEFT JOIN
		dbo.#Results_1 B 
			on a.CALLID=b.CALLID
		LEFT JOIN dbo.#Results4 C 
			ON a.CALLID=c.CALLID
WHERE B.LTOHIT=0 AND A.CXLANGTIME>=C.OPNTIME) D
WHERE D.RNK1=1

-- Calculating Befor & After Consumer 
--Language for Down Payment
IF OBJECT_ID('tempdb.dbo.#Results_5') IS NOT NULL 
DROP TABLE dbo.#Results_5
SELECT A.CALLID
	  ,A.CATHIT
	  ,A.COMPNAME
	  ,A.CXLANGTIME
	  ,B.DPSTTIME
	  ,CASE WHEN B.DPSTTIME<=A.CXLANGTIME THEN A.COMPNAME END CXCOMPAFTERDP
	  ,CASE WHEN B.DPSTTIME<=A.CXLANGTIME THEN A.CXLANGTIME END CXSTTIMEAFTERDP
	  ,CASE WHEN B.DPSTTIME>A.CXLANGTIME THEN A.COMPNAME END CXCOMPBEFDP
	  ,CASE WHEN B.DPSTTIME>=A.CXLANGTIME THEN A.CXLANGTIME END CXSTTIMEBEFDP
INTO dbo.#Results_5
FROM dbo.#Results9 a LEFT JOIN
		dbo.#Results_1 B 
			on a.CALLID=b.CALLID
WHERE B.DPHIT=1

-- Calculating First instance of   Consumer 
--Language After Down payment
IF OBJECT_ID('tempdb.dbo.#Results_5_1') IS NOT NULL 
DROP TABLE dbo.#Results_5_1
SELECT B.CALLID
	  ,B.CATHIT AS CXCATHITAFTERDP
	  ,B.CXCOMPAFTERDP
	  ,B.CXSTTIMEAFTERDP
INTO dbo.#Results_5_1
FROM (
SELECT A.CALLID
	  ,A.CATHIT
	  ,A.CXCOMPAFTERDP
	  ,A.CXSTTIMEAFTERDP
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.CXSTTIMEAFTERDP) RNK1
FROM dbo.#Results_5 A
WHERE A.CXCOMPAFTERDP IS NOT NULL) B
WHERE B.RNK1=1

-- Calculating First instance of   Consumer 
--Language Before Down Payment
IF OBJECT_ID('tempdb.dbo.#Results_5_2') IS NOT NULL 
DROP TABLE dbo.#Results_5_2
SELECT B.CALLID
	  ,B.CXCATHITBEFDP
	  ,B.CXCOMPBEFDP
	  ,B.CXSTTIMEBEFDP
INTO dbo.#Results_5_2
FROM
(SELECT A.CALLID
	  ,A.CATHIT AS CXCATHITBEFDP
	  ,A.COMPNAME
	  ,A.CXCOMPBEFDP
	  ,A.CXSTTIMEBEFDP
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY (A.CXLANGTIME-A.CXSTTIMEBEFDP)) RNK1
FROM dbo.#Results_5 A
WHERE A.CXSTTIMEBEFDP IS NOT NULL) B
WHERE RNK1=1

-- Calculating Befor & After Consumer 
--Language for Second Voice
IF OBJECT_ID('tempdb.dbo.#Results_6') IS NOT NULL 
DROP TABLE dbo.#Results_6
SELECT A.CALLID
	  ,A.CATHIT
	  ,A.COMPNAME
	  ,A.CXLANGTIME
	  ,B.SVCSTTIME
	  ,CASE WHEN B.SVCSTTIME<=A.CXLANGTIME THEN A.COMPNAME END CXCOMPAFTERSVC
	  ,CASE WHEN B.SVCSTTIME<=A.CXLANGTIME THEN A.CXLANGTIME END CXSTTIMEAFTERSVC
	  ,CASE WHEN B.SVCSTTIME>A.CXLANGTIME THEN A.COMPNAME END CXCOMPBEFSVC
	  ,CASE WHEN B.SVCSTTIME>=A.CXLANGTIME THEN A.CXLANGTIME END CXSTTIMEBEFSVC
INTO dbo.#Results_6
FROM dbo.#Results9 a LEFT JOIN
		dbo.#Results_1 B 
			on a.CALLID=b.CALLID
WHERE B.SVCHIT=1

-- Calculating First instance of   Consumer 
--Language After Second Voice
IF OBJECT_ID('tempdb.dbo.#Results_6_1') IS NOT NULL 
DROP TABLE dbo.#Results_6_1
SELECT B.CALLID
	  ,B.CATHIT AS CXCATHITAFTERSVC
	  ,B.CXCOMPAFTERSVC
	  ,B.CXSTTIMEAFTERSVC
INTO dbo.#Results_6_1
FROM (
SELECT A.CALLID
	  ,A.CATHIT
	  ,A.CXCOMPAFTERSVC
	  ,A.CXSTTIMEAFTERSVC
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY A.CXSTTIMEAFTERSVC) RNK1
FROM dbo.#Results_6 A
WHERE A.CXCOMPAFTERSVC IS NOT NULL) B
WHERE B.RNK1=1

-- Calculating First instance of   Consumer 
--Language Before Second Voice
IF OBJECT_ID('tempdb.dbo.#Results_6_2') IS NOT NULL 
DROP TABLE dbo.#Results_6_2
SELECT B.CALLID
	  ,B.CXCATHITBEFSVC
	  ,B.CXCOMPBEFSVC
	  ,B.CXSTTIMEBEFSVC
INTO dbo.#Results_6_2
FROM
(SELECT A.CALLID
	  ,A.CATHIT AS CXCATHITBEFSVC
	  ,A.COMPNAME
	  ,A.CXCOMPBEFSVC
	  ,A.CXSTTIMEBEFSVC
	  ,ROW_NUMBER() OVER (PARTITION BY A.CALLID ORDER BY (A.CXLANGTIME-A.CXSTTIMEBEFSVC)) RNK1
FROM dbo.#Results_6 A
WHERE A.CXSTTIMEBEFSVC IS NOT NULL) B
WHERE RNK1=1

--Combining All the data in One Table for final calculations
IF OBJECT_ID('tempdb.dbo.#Results_7') IS NOT NULL 
DROP TABLE dbo.#Results_7
SELECT A.EOM
	  ,A.AGNTID
	  ,A.CALLID
	  ,A.CALLDT
	  ,A.RGSSEID
	  ,A.DIS1
	  ,A.RECDP
	  ,A.PHNUMB
	  ,A.CLNTID
	  ,A.SILT
	  ,A.CLDUR
	  ,A.RGSACC
	  ,A.DIR
	  ,A.DIS1CAT
	  ,B.OPNTIME
	  ,C.NEGSTTIME
	  ,C.NEGENDTIME
	  ,D.LTOHIT
	  ,D.LTOSTTIME
	  ,D.DPHIT
	  ,D.DPSTTIME
	  ,D.SVCHIT
	  ,D.SVCSTTIME
	  ,e.CATHIT AS CXFSTBEHACAT
	  ,e.COMPNAME AS CXFSTBEHACOMP
	  ,e.CXLANGTIME AS CXFSTBEHATME
	  ,f.CXCATHITAFTERLTO
	  ,f.CXCOMPAFTERLTO
	  ,f.CXSTTIMEAFTERLTO
	  ,g.CXCATHITBEFLTO
	  ,g.CXCOMPBEFLTO
	  ,g.CXSTTIMEBEFLTO
	  ,h.CXCATHITNONLTO
	  ,h.CXCOMPNONLTO
	  ,h.CXSTTIMENONLTO
	  ,i.CXCATHITAFTERDP
	  ,i.CXCOMPAFTERDP
	  ,i.CXSTTIMEAFTERDP
	  ,j.CXCATHITBEFDP
	  ,j.CXCOMPBEFDP
	  ,j.CXSTTIMEBEFDP
	  ,k.CXCATHITAFTERSVC
	  ,k.CXCOMPAFTERSVC
	  ,k.CXSTTIMEAFTERSVC
	  ,l.CXCATHITBEFSVC
	  ,l.CXCOMPBEFSVC
	  ,l.CXSTTIMEBEFSVC
INTO dbo.#Results_7
FROM dbo.#Results a 
	LEFT JOIN dbo.#Results4 b 
		ON a.CALLID=B.CALLID
	LEFT JOIN dbo.#Results8_2 c
		ON A.CALLID=C.CALLID
	LEFT JOIN dbo.#Results_1 D
		ON A.CALLID=D.CALLID
	LEFT JOIN dbo.#Results_2 e
		ON a.CALLID=e.CALLID
	LEFT JOIN dbo.#Results_3_1 f
		ON a.CALLID=f.CALLID
	LEFT JOIN dbo.#Results_3_2 g
		ON a.CALLID=g.CALLID
	LEFT JOIN dbo.#Results_4 h
		ON a.CALLID=h.CALLID
	LEFT JOIN dbo.#Results_5_1 i
		ON a.CALLID=i.CALLID
	LEFT JOIN dbo.#Results_5_2 j
		ON a.CALLID=j.CALLID
	LEFT JOIN dbo.#Results_6_1 k
		on a.CALLID=k.CALLID
	LEFT JOIN dbo.#Results_6_2 l
		on a.CALLID=l.CALLID

--CALculating first behaviour 
IF OBJECT_ID('tempdb.dbo.#Results_7_1') IS NOT NULL 
DROP TABLE dbo.#Results_7_1
SELECT DISTINCT	 a.EOM
				,a.AGNTID
				,a.CALLID
				,a.CALLDT
				,a.RGSSEID
				,a.DIS1
				,a.RECDP
				,a.PHNUMB
				,a.CLNTID
				,a.SILT
				,a.CLDUR
				,a.RGSACC
				,a.DIR
				,a.DIS1CAT
				,a.OPNTIME
				,a.NEGSTTIME
				,a.NEGENDTIME
				,a.LTOHIT
				,a.LTOSTTIME
				,a.DPHIT
				,a.DPSTTIME
				,a.SVCHIT
				,a.SVCSTTIME
				,CASE WHEN a.LTOHIT=1 AND a.CXFSTBEHACAT IS NULL 
					THEN a.CXCATHITBEFLTO 
					WHEN a.LTOHIT=0 AND a.CXFSTBEHACAT IS NULL 
					THEN a.CXCATHITNONLTO 
					ELSE a.CXFSTBEHACAT END AS CXFSTBEHACAT
				,CASE WHEN a.LTOHIT=1 AND a.CXFSTBEHACAT IS NULL 
					THEN a.CXCOMPBEFLTO
					WHEN a.LTOHIT=0 AND a.CXFSTBEHACAT IS NULL 
					THEN a.CXCOMPNONLTO ELSE a.CXFSTBEHACOMP END AS CXFSTBEHACOMP
				,CASE WHEN LTOHIT=1 AND a.CXFSTBEHACAT IS NULL 
					THEN a.CXSTTIMEBEFLTO 
					WHEN a.LTOHIT=0 AND a.CXFSTBEHACAT IS NULL 
					THEN a.CXSTTIMENONLTO ELSE a.CXFSTBEHATME END AS CXFSTBEHATME
				,a.CXCATHITAFTERLTO
				,a.CXCOMPAFTERLTO
				,a.CXSTTIMEAFTERLTO
				,a.CXCATHITAFTERDP
				,a.CXCOMPAFTERDP
				,a.CXSTTIMEAFTERDP
				,a.CXCATHITBEFDP
				,a.CXCOMPBEFDP
				,a.CXSTTIMEBEFDP
				,a.CXCATHITAFTERSVC
				,a.CXCOMPAFTERSVC
				,a.CXSTTIMEAFTERSVC
				,a.CXCATHITBEFSVC
				,a.CXCOMPBEFSVC
				,a.CXSTTIMEBEFSVC	
INTO dbo.#Results_7_1
FROM dbo.#Results_7 a 

-- Adding Category groups
IF OBJECT_ID('tempdb.dbo.#Results_7_2') IS NOT NULL 
DROP TABLE dbo.#Results_7_2
SELECT DISTINCT	 a.EOM
				,a.AGNTID
				,a.CALLID
				,a.CALLDT
				,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
					a.RGSSEID,'T.mp3',''),'D.mp3',''),'DN.mp3',''),'.mp3',''),'_','@') RGSSEID
				,a.DIS1
				,a.RECDP
				,a.PHNUMB
				,a.CLNTID
				,a.SILT
				,a.CLDUR
				,a.RGSACC
				,a.DIR
				,a.DIS1CAT
				,a.OPNTIME
				,a.NEGSTTIME
				,a.NEGENDTIME
				,a.LTOHIT
				,a.LTOSTTIME
				,a.DPHIT
				,a.DPSTTIME
				,a.SVCHIT
				,a.SVCSTTIME
				,a.CXFSTBEHACAT
				,a.CXFSTBEHACOMP
				,a.CXFSTBEHATME
				,a.CXCATHITAFTERLTO
				,a.CXCOMPAFTERLTO
				,a.CXSTTIMEAFTERLTO
				,a.CXCATHITAFTERDP
				,a.CXCOMPAFTERDP
				,a.CXSTTIMEAFTERDP
				,a.CXCATHITBEFDP
				,a.CXCOMPBEFDP
				,a.CXSTTIMEBEFDP
				,a.CXCATHITAFTERSVC
				,a.CXCOMPAFTERSVC
				,a.CXSTTIMEAFTERSVC
				,a.CXCATHITBEFSVC
				,a.CXCOMPBEFSVC
				,a.CXSTTIMEBEFSVC	
				,b.CATHITCATG AS CXFSTBEHAGRP
				,c.CATHITCATG AS CXAFTERLTOGRP
				,d.CATHITCATG AS CXBEFDPGRP
				,e.CATHITCATG AS CXAFTERDPGRP
				,f.CATHITCATG AS CXBEFSVCGRP
				,g.CATHITCATG AS CXAFTERSVCGRP
				,h.COMPNAME AS AGNTCOMPLTO
				,i.COMPNAME AS AGNTCOMPDP
				,CASE WHEN DAY([CALLDT])<=7 THEN 'Week 1' 
					  WHEN DAY([CALLDT])>7 AND DAY([CALLDT])<15 THEN 'Week 2'
					  WHEN DAY([CALLDT])>14 AND DAY([CALLDT])<22 THEN 'Week 3' 
					  ELSE 'Week 4' END AS WKNUMB                                
				,CASE WHEN CLDUR<=90 THEN 'A.00-90 Secs' 
					  WHEN CLDUR>91 AND CLDUR<180 THEN 'B.91-180 Secs'
					  WHEN CLDUR>181 AND CLDUR<270 THEN 'C.181-270 Secs'
					  WHEN CLDUR>271 AND CLDUR<360 THEN 'D.271-360 Secs' 
					  ELSE 'E.361+ Secs' END AS STRNBG
INTO dbo.#Results_7_2
FROM dbo.#Results_7_1 a LEFT JOIN 
	dbo.#Results3 b 
		ON a.CXFSTBEHACAT=b.CATHIT
			AND a.CXFSTBEHACOMP=b.COMPNAME
	LEFT JOIN dbo.#Results3 c 
		ON a.CXCATHITAFTERLTO=c.CATHIT
			AND a.CXCOMPAFTERLTO=c.COMPNAME
	LEFT JOIN dbo.#Results3 d
		ON a.CXCATHITBEFDP=d.CATHIT
			AND a.CXCOMPBEFDP=d.COMPNAME
	LEFT JOIN dbo.#Results3 e 
		ON a.CXCATHITAFTERDP=e.CATHIT
			AND a.CXCOMPAFTERDP=e.COMPNAME
	LEFT JOIN dbo.#Results3 f
		ON a.CXCATHITBEFSVC=f.CATHIT
			AND a.CXCOMPBEFSVC=f.COMPNAME
	LEFT JOIN dbo.#Results3 g
		ON a.CXCATHITAFTERSVC=g.CATHIT
			AND a.CXCOMPAFTERSVC=g.COMPNAME
	LEFT JOIN dbo.#Results7 h
		ON a.CALLID=h.CALLID
	LEFT JOIN dbo.#Results5 i
		ON a.CALLID=i.CALLID


DELETE FROM Speech_Analytics.dbo.CM_Rpt_AXPLegalTO
WHERE EOM=@End_Month

 INSERT INTO Speech_Analytics.dbo.CM_Rpt_AXPLegalTO
 SELECT  EOM
		,AGNTID
		,CALLID
		,CALLDT
		,RGSSEID
		,DIS1
		,RECDP
		,PHNUMB
		,CLNTID
		,SILT
		,CLDUR
		,RGSACC
		,DIR
		,DIS1CAT
		,OPNTIME
		,NEGSTTIME
		,NEGENDTIME
		,LTOHIT
		,LTOSTTIME
		,DPHIT
		,DPSTTIME
		,SVCHIT
		,SVCSTTIME
		,CXFSTBEHACAT
		,CXFSTBEHACOMP
		,CXFSTBEHATME
		,CXCATHITAFTERLTO
		,CXCOMPAFTERLTO
		,CXSTTIMEAFTERLTO
		,CXCATHITAFTERDP
		,CXCOMPAFTERDP
		,CXSTTIMEAFTERDP
		,CXCATHITBEFDP
		,CXCOMPBEFDP
		,CXSTTIMEBEFDP
		,CXCATHITAFTERSVC
		,CXCOMPAFTERSVC
		,CXSTTIMEAFTERSVC
		,CXCATHITBEFSVC
		,CXCOMPBEFSVC
		,CXSTTIMEBEFSVC
		,CXFSTBEHAGRP
		,CXAFTERLTOGRP
		,CXBEFDPGRP
		,CXAFTERDPGRP
		,CXBEFSVCGRP
		,CXAFTERSVCGRP
		,AGNTCOMPLTO
		,AGNTCOMPDP
		,WKNUMB
		,STRNBG
FROM dbo.#Results_7_2


DECLARE @tab char(1) = CHAR(9)

DECLARE @totcntmm VARCHAR (MAX);
SELECT @totcntmm=CAST(COUNT(CALLID) AS varchar) FROM dbo.#Results_7_2
PRINT @totcntmm

DECLARE @body1 VARCHAR (MAX); 
		SET @body1 = 'Hi All,
		
Daily AXP Legal Talk off SQL code executed for '+convert(varchar,@End_Month,102)+'

Total count of rows added are '+ (@totcntmm) +'. 

Regards,
Business Analytics';
		print @body1
DECLARE @subject1 VARCHAR (MAX); 
		SET @subject1 = 'Daily AXP Legal Talk off SQL code executed for ' + convert(varchar,@End_Month,102);
		print @subject1
	--send email
	if (SELECT count(CALLID) FROM dbo.#Results_7_2)>0
		EXEC msdb.dbo.sp_send_dbmail
		@profile_name = 'DW Mail',--@@SERVERNAME, --'DFW2-BISQL-001',
		@from_address ='Reports SpeechAnalytics <reports.speechanalytics@radiusgs.com>',
		@recipients = 'dw@radiusgs.com;business.analytics@radiusgs.com',
		@copy_recipients='Pulkit.Jain@radiusgs.com;',
		@subject = @subject1,
		@body = @body1;
	end