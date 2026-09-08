USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[sp_ins_Client_USBank_Vendor_Pay]    Script Date: 9/6/2023 10:58:48 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROC [dbo].[sp_ins_Client_USBank_Vendor_Pay]
AS
SET NOCOUNT ON;

DECLARE @dDateRangeEnd		DATE = EOMONTH(GetDate(),-1);
DECLARE @dDateRangeStart	DATE = DATEADD(MONTH,-12,DATEADD(DAY,1,@dDateRangeEnd));

DECLARE @dDateRangeEndPDH	DATE = EOMONTH(GetDate());
DECLARE @dDateRangeStartPDH	DATE = DATEADD(MONTH,-12,DATEADD(DAY,1,@dDateRangeEndPDH));

/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tPlacements';

IF OBJECT_ID('TempDB..#tPlacements') IS NOT NULL DROP TABLE #tPlacements;

SELECT
 DATEADD(DAY,1,EOMONTH(C.List_Date,(-1))) Month_Start_Date
,C.CLIENT_ID
,C.CUSTOMER_ID
,CANCEL_DATE
,ISNULL(C.CANCEL_CODE,'')			CANCEL_CODE
,ISNULL(C.ORIGINAL_CANCEL_CODE,'')	ORIGINAL_CANCEL_CODE
,CASE WHEN 'SIF' IN (CANCEL_CODE,ORIGINAL_CANCEL_CODE) THEN (B.INITIAL_BALANCE * 0.4) ELSE NULL END SIF_AMOUNT
,B.SIF_AMOUNT SIFAMNT
,B.INITIAL_BALANCE
,B.PRINCIPAL_BALANCE
,B.AMT_PAID_ON_ACCOUNT
INTO #tPlacements
FROM DW_MSTR_DM.dbo.LU_CUSTOMER (NOLOCK) C
LEFT JOIN DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT (NOLOCK) B ON C.CUSTOMER_ID = B.CUSTOMER_ID AND C.CLIENT_ID = B.CLIENT_ID
WHERE C.List_Date BETWEEN @dDateRangeStart AND @dDateRangeEnd
AND C.CLIENT_ID IN ('UBN11','UBN12','UBN8','UBN9', 'UBN41', 'UBN48', 'UBN49','UBN18','UBN19','UBN21','UBN22','UBN23')

CREATE UNIQUE INDEX #inxPlacements ON #tPlacements(Month_Start_Date ASC,CLIENT_ID ASC,CUSTOMER_ID ASC);
-- 00:01:32
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tPF';

IF OBJECT_ID('TempDB..#tPF') IS NOT NULL DROP TABLE #tPF;

SELECT
DATEADD(DAY,1,EOMONTH(C.List_Date,(-1)))	Month_Start_Date
,C.CLIENT_ID
,C.CUSTOMER_ID
,ISNULL(B.PAYMENT_AMT_APPLIED,0)			PAYMENT_AMT_APPLIED
INTO #tPF
FROM DW_MSTR_DM.dbo.LU_CUSTOMER (NOLOCK) C
LEFT JOIN DW_MSTR_DM.dbo.PAYMENT_FACT (NOLOCK) B ON C.CUSTOMER_ID = B.CUSTOMER_ID AND C.CLIENT_ID = B.CLIENT_ID
WHERE C.List_Date BETWEEN @dDateRangeStart AND @dDateRangeEnd
AND B.PYMT_TYPE NOT IN ('DBJ','CRJ','PCK','CAN'); 

CREATE INDEX #inxPF ON #tPF(Month_Start_Date ASC,CLIENT_ID ASC,CUSTOMER_ID ASC);
-- 00:00:12
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tPDH';

IF OBJECT_ID('TempDB..#tPDH') IS NOT NULL DROP TABLE #tPDH;

SELECT
DATEADD(DAY,1,EOMONTH(IMPORT_DATE,-2)) Month_Start_Date,
MIN(IMPORT_DATE) IMPORT_DATE
INTO #tPDH
FROM DW_MSTR_DM.dbo.TBL_Customer_PostDates_HISTORY (NOLOCK)
WHERE IMPORT_DATE BETWEEN @dDateRangeStartPDH AND @dDateRangeEndPDH
GROUP BY DATEADD(DAY,1,EOMONTH(IMPORT_DATE,-2));

CREATE UNIQUE INDEX #inxPDH ON #tPDH(IMPORT_DATE ASC);
-- 00:00:30
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';+`
-- PRINT '#tPPP';

IF OBJECT_ID('TempDB..#tPPP') IS NOT NULL DROP TABLE #tPPP;

SELECT
 P.Month_Start_Date
,H.CLIENT_ID
,H.CUSTOMER_ID
,H.PROMISE_PAYMENT
INTO #tPPP
FROM DW_MSTR_DM.dbo.TBL_Customer_PostDates_HISTORY (NOLOCK) H 
JOIN #tPDH P ON H.IMPORT_DATE = P.IMPORT_DATE

CREATE INDEX #inxPPP ON #tPPP(Month_Start_Date ASC,CLIENT_ID ASC,CUSTOMER_ID ASC);
-- 00:05:51
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tPlmtSUM';

IF OBJECT_ID('TempDB..#tPlmtSUM') IS NOT NULL DROP TABLE #tPlmtSUM;

SELECT
Month_Start_Date,
CLIENT_ID,
COUNT(DISTINCT CUSTOMER_ID)		Plmnt_Num,
SUM(ISNULL(INITIAL_BALANCE,0))	Plmnt_Amt
INTO #tPlmtSUM
FROM #tPlacements 
GROUP BY Month_Start_Date,CLIENT_ID;

CREATE UNIQUE INDEX #inxPlmtSUM ON #tPlmtSUM(Month_Start_Date ASC,CLIENT_ID ASC);
--00:00:01
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tSIFnull';

IF OBJECT_ID('TempDB..#tSIFnull') IS NOT NULL DROP TABLE #tSIFnull;

SELECT 
Month_Start_Date,
Client_ID,
CUSTOMER_ID 
INTO #tSIFnull
FROM #tPlacements 
WHERE SIF_AMOUNT IS NULL 
GROUP BY Month_Start_Date,Client_ID,CUSTOMER_ID;  

CREATE UNIQUE INDEX #inxSIFnull ON #tSIFnull(Month_Start_Date ASC,CLIENT_ID ASC,CUSTOMER_ID ASC);
-- 00:00:06
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#pppSUM';

IF OBJECT_ID('TempDB..#tPPPsum') IS NOT NULL DROP TABLE #tPPPsum;

SELECT
 S.Month_Start_Date
,H.CLIENT_ID
,COUNT(DISTINCT H.CUSTOMER_ID)		PPP_Num
,SUM(ISNULL(H.PROMISE_PAYMENT,0))	PPP_Amt 
INTO #tPPPsum
FROM #tPPP H 
JOIN #tSIFnull S 
ON H.Month_Start_Date = S.Month_Start_Date AND H.CLIENT_ID = S.CLIENT_ID AND H.CUSTOMER_ID = S.CUSTOMER_ID 
GROUP BY S.Month_Start_Date,H.CLIENT_ID;

CREATE UNIQUE INDEX #inxPPPsum ON #tPPPsum(Month_Start_Date ASC,CLIENT_ID ASC);
-- 00:00:06
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tSIFsum';

IF OBJECT_ID('TempDB..#tSIFsum') IS NOT NULL DROP TABLE #tSIFsum;

SELECT
Month_Start_Date,
CLIENT_ID,
COUNT(DISTINCT CUSTOMER_ID)	SIF_Num,
SUM(SIF_AMOUNT)				SIF_Amt
INTO #tSIFsum
FROM #tPlacements
WHERE SIF_AMOUNT IS NOT NULL
GROUP BY Month_Start_Date,CLIENT_ID;

CREATE UNIQUE INDEX #inxSIFsum ON #tSIFsum(Month_Start_Date ASC,CLIENT_ID ASC);
-- 00:00:01
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tSIFnullBal';

IF OBJECT_ID('TempDB..#tSIFnullBal') IS NOT NULL DROP TABLE #tSIFnullBal;

SELECT 
Month_Start_Date,
Client_ID,
CUSTOMER_ID 
INTO #tSIFnullBal
FROM #tPlacements 
WHERE SIF_AMOUNT IS NULL AND PRINCIPAL_BALANCE = 0
GROUP BY Month_Start_Date,Client_ID,CUSTOMER_ID  

CREATE UNIQUE INDEX #inxSIFnullBal ON #tSIFnullBal(Month_Start_Date ASC,CLIENT_ID ASC,CUSTOMER_ID ASC);
-- 00:00:05
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
-- PRINT '';
-- PRINT '#tBIFsum';

IF OBJECT_ID('TempDB..#tBIFsum') IS NOT NULL DROP TABLE #tBIFsum;

SELECT
 F.Month_Start_Date
,F.CLIENT_ID
,COUNT(DISTINCT F.CUSTOMER_ID)			BIF_Num
,SUM(ISNULL(F.PAYMENT_AMT_APPLIED,0))	BIF_AMT
INTO #tBIFsum
FROM #tPF F 
JOIN #tSIFnullBal B
ON B.Month_Start_Date = F.Month_Start_Date AND B.CLIENT_ID = F.CLIENT_ID AND B.CUSTOMER_ID = F.CUSTOMER_ID 
GROUP BY F.Month_Start_Date,F.CLIENT_ID;

CREATE INDEX #inxBIFsum ON #tBIFsum(Month_Start_Date ASC,CLIENT_ID ASC);
-- 00:00:01
/*-----------------------------------------------------------------------------------------------------------*/


/*************************************************************************************************************/
TRUNCATE TABLE dbo.Client_USBank_Vendor_Pay;

INSERT dbo.Client_USBank_Vendor_Pay
(
 [Month_Start_Date]
,[Client_ID]
,[Client_Parent]
,[Stream_Name]
,[Placements_Num]
,[Placements_Amt]
,[PPP_Acct_Num]
,[PPP_Acct_Amt]
,[SIF_Acct_Num]
,[SIF_Acct_Amt]
,[BIF_Acct_Num]
,[BIF_Acct_Amt]
)
SELECT
PL.Month_Start_Date			Month_Start_Date,
PL.Client_ID				Client_ID,
ISNULL(S.Parent,'')			Client_Parent,
ISNULL(S.Client_Stream,'')	Stream_Name,
PL.Plmnt_Num				Placements_Num,
PL.Plmnt_Amt				Placements_Amt,
ISNULL(PP.PPP_Num,0)		PPP_Acct_Num,
ISNULL(PP.PPP_Amt,0)		PPP_Acct_Amt,
ISNULL(SF.SIF_Num,0)		SIF_Acct_Num,
ISNULL(SF.SIF_Amt,0)		SIF_Acct_Amt,
ISNULL(BF.BIF_Num,0)		BIF_Acct_Num,
ISNULL(BF.BIF_AMT,0)		BIF_Acct_Amt
FROM #tPlmtSUM PL 
LEFT JOIN DW_MSTR_DM.dbo.TblClientStreams (NOLOCK) S ON				   PL.Client_ID =  S.Client_ID
LEFT JOIN #tPPPsum PP ON PL.Month_Start_Date = PP.Month_Start_Date AND PL.Client_ID = PP.CLIENT_ID
LEFT JOIN #tSIFsum SF ON PL.Month_Start_Date = SF.Month_Start_Date AND PL.Client_ID = SF.CLIENT_ID
LEFT JOIN #tBIFsum BF ON PL.Month_Start_Date = BF.Month_Start_Date AND PL.Client_ID = BF.CLIENT_ID;
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_Client_USBank_Vendor_Pay] TO [CORP\aramugade]
    AS [dbo];

