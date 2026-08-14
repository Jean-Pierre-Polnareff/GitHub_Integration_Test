USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[usp_Insert_RPT_Postdate_25Mth_USBank]    Script Date: 5/12/2025 1:58:08 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Object: dbo.usp_Insert_RPT_Postdate_25Mth_USBank
-- Create date: 03/27/2023
--
--  Description: Provides data for PDCNext25Mth for USBank
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	03/27/2023	 Created
-- =============================================

ALTER PROCEDURE [dbo].[usp_Insert_RPT_Postdate_25Mth_USBank] 

AS

SET NOCOUNT ON;

/***************************************************************************
	Create temp table to hold 25 months to be included in report
***************************************************************************/
IF OBJECT_ID('Tempdb..#Months') IS NOT NULL
    DROP TABLE #Months;

--Get 25 months to be reported in monthly fields
SELECT TOP 25 --12
        ld.MONTH_DATE
       ,ld.Month_ID
       ,MonthOrder = ROW_NUMBER() OVER (ORDER BY ld.Month_ID ASC)
INTO    #Months
FROM    (SELECT DISTINCT
                MONTH_DATE
               ,Month_ID
         FROM   DW_MSTR_DM.dbo.LU_DATE (NOLOCK)
         WHERE  CALNDR_DT >= CAST(DATEADD(DAY,-1,GETDATE()) AS DATE)) ld;

        
--SELECT * FROM #Months
/***********************************************************
	Create temp tables to get promises by month
**********************************************************/
IF OBJECT_ID('tempdb..#PromiseByMonth') IS NOT NULL
DROP TABLE #PromiseByMonth; 
-----------------------------------------For FACS-----------------------------------------
SELECT  SourceSystem = 'FACS'
       ,KeySourceSystem = 4
       ,tcs.Client_Stream
       ,tcs.Client_ID
       ,lc.EMPLOYEE_ID
       ,tcpd.CUSTOMER_ID
	   ,KeyCustomer = NULL
       ,lc.LAST_NAME
       ,lc.STATUS_CODE
       ,CurrBalance = (ISNULL(obf.PRINCIPAL_BALANCE,0.00) + ISNULL(obf.AGENCY_INTEREST_BALANCE,0.00)
            + ISNULL(obf.INTEREST_BALANCE,0.00) + ISNULL(obf.LIST3_BALANCE,0.00) + ISNULL(obf.LIST4_BALANCE,0.00))
       ,PostdateMonth = ld.Month_ID
       ,MthOrder = ISNULL(m.MonthOrder,26) ---ISNULL(m.MonthOrder,25)
       ,NumRemaining = COUNT(*) ---include all future pdc, not just next 25 months
       ,MonthlyAmount = SUM(tcpd.PROMISE_PAYMENT)  
	   ,lc.LIST_DATE
	   ,tcs.Commission     
INTO    #PromiseByMonth
FROM    DW_MSTR_DM.dbo.TBL_Customer_PostDates tcpd (NOLOCK)
JOIN    DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK)
        ON tcpd.CLIENT_ID = tcs.Client_ID
JOIN    DW_MSTR_DM.dbo.LU_CUSTOMER lc (NOLOCK)
        ON tcpd.CUSTOMER_ID = lc.CUSTOMER_ID
JOIN    DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT obf (NOLOCK)
        ON lc.CUSTOMER_ID = obf.CUSTOMER_ID
JOIN    DW_MSTR_DM.dbo.LU_DATE ld (NOLOCK)
        ON ld.CALNDR_DT = tcpd.PROMISE_PAY_DATE
LEFT JOIN #Months m
		ON ld.MONTH_DATE = m.MONTH_DATE
---WHERE tcpd.PROMISE_PAY_DATE >= DATEADD(day, DATEDIFF(day, 0, GETDATE()), 0)
where tcs.client_id in('UBN11', 'UBN12', 'UBN14', 'UBN18', 'UBN19', 'UBN21', 'UBN7', 'UBN8', 'UBN9'
                                                                        ,'UBN41', 'UBN48', 'UBN49'             ---- addition of 3 clientids as per ticket No.490976 
																		,'UBN22', 'UBN23')                     ---- addition of 2 clientids as per ticket No.724938              
GROUP BY tcs.Client_Stream
       ,tcs.Client_ID
       ,lc.EMPLOYEE_ID
       ,tcpd.CUSTOMER_ID
       ,lc.LAST_NAME
       ,lc.STATUS_CODE
       ,(ISNULL(obf.PRINCIPAL_BALANCE,0.00) + ISNULL(obf.AGENCY_INTEREST_BALANCE,0.00) + ISNULL(obf.INTEREST_BALANCE,0.00)
         + ISNULL(obf.LIST3_BALANCE,0.00) + ISNULL(obf.LIST4_BALANCE,0.00))
       ,ld.Month_ID
       ,m.MonthOrder
	   ,lc.LIST_DATE
	   ,tcs.Commission 

	  
----SELECT * FROM #PromiseByMonth

/*******************************************************
	Flatten out the 25 months and populate with monthly total
********************************************************/
IF OBJECT_ID('tempdb..#FutureByMonth') IS NOT NULL
DROP TABLE #FutureByMonth; 

SELECT  *
INTO    #FutureByMonth
FROM    (SELECT SourceSystem
               ,KeySourceSystem
               ,Client_Stream
               ,Client_ID
               ,EMPLOYEE_ID
               ,CUSTOMER_ID
			   ,KeyCustomer
               ,LAST_NAME
               ,CurrBalance
               ,STATUS_CODE
               ,PostdateMonth
               ,MonthlyAmount
               ,MthOrder
			   ,LIST_DATE
	           ,Commission 
         FROM   #PromiseByMonth) cbc PIVOT( SUM(MonthlyAmount) FOR MthOrder IN ([1],[2],[3],[4],[5],[6],[7],[8],[9],[10],
                                                                                [11],[12],[13],[14],[15],[16],[17],[18],[19],[20],
																				[21],[22],[23],[24],[25],[26]) )AS PivotAmt;

--SELECT * FROM #FutureByMonth fbm
/*******************************************************
--Look back at payments on PDC accounts since 1/1/16. 
	These will be consider as PDCs 'Taken'
********************************************************/
IF OBJECT_ID('tempdb..#Payments') IS NOT NULL
    DROP TABLE #Payments; 
-----------------------------------------For FACS-----------------------------------------
SELECT  
fbm.sourcesystem , fbm.Keysourcesystem , fbm.CUSTOMER_ID, fbm.KeyCustomer
       ,NumPaid = COUNT(*)
INTO    #Payments
FROM    (SELECT DISTINCT sourcesystem , Keysourcesystem, CUSTOMER_ID, KeyCustomer FROM #FutureByMonth where KeySourceSystem = 4) fbm
JOIN    DW_MSTR_DM.dbo.PAYMENT_FACT pf
        ON fbm.CUSTOMER_ID = pf.CUSTOMER_ID
WHERE   pf.PYMT_DATE >= '2016-01-01'
        AND pf.PYMT_TYPE NOT IN ('CAN','PCK','ADJ','DBJ','NSF','COR')
GROUP BY fbm.sourcesystem , fbm.Keysourcesystem , fbm.CUSTOMER_ID , fbm.KeyCustomer


--SELECT * FROM #Payments
/*******************************************************
--Get Num Posted and num remaining
********************************************************/
IF OBJECT_ID('tempdb..#Counts') IS NOT NULL
DROP TABLE #Counts; 

SELECT r.SourceSystem , r.KeySourceSystem , r.KeyCustomer
       ,r.CUSTOMER_ID
       ,NumPmts = ISNULL(p.NumPaid,0)
       ,r.PDC_Remaining
       ,TotalTaken = ISNULL(p.NumPaid,0) + r.PDC_Remaining
	   
INTO    #Counts
FROM    (SELECT sourcesystem , Keysourcesystem, CUSTOMER_ID, KeyCustomer
               ,PDC_Remaining = SUM(NumRemaining)
         FROM   #PromiseByMonth
         GROUP BY sourcesystem , Keysourcesystem, CUSTOMER_ID, KeyCustomer) r
LEFT JOIN #Payments p
        ON p.CUSTOMER_ID = r.CUSTOMER_ID
		AND p.KeySourceSystem = r.KeySourceSystem
		AND p.SourceSystem = r.SourceSystem
		---AND p.KeyCustomer = r.KeyCustomer;

--SELECT * FROM #Counts 

IF OBJECT_ID('tempdb..#t') IS NOT NULL
DROP TABLE #t; 

SELECT  fbm.SourceSystem
       ,fbm.KeySourceSystem
       ,fbm.Client_Stream
       ,fbm.Client_ID
       ,fbm.Employee_ID
       ,fbm.CUSTOMER_ID
	   ,fbm.KeyCustomer
       ,fbm.LAST_NAME
       ,fbm.CurrBalance    
       ,fbm.STATUS_CODE
	   ,fbm.LIST_DATE
	   ,fbm.Commission
       ,c.TotalTaken
       ,c.PDC_Remaining
       ,Month1 = SUM(ISNULL(fbm.[1],0))
       ,Month2 = SUM(ISNULL(fbm.[2],0))
       ,Month3 = SUM(ISNULL(fbm.[3],0))
       ,Month4 = SUM(ISNULL(fbm.[4],0))
       ,Month5 = SUM(ISNULL(fbm.[5],0))
       ,Month6 = SUM(ISNULL(fbm.[6],0))
       ,Month7 = SUM(ISNULL(fbm.[7],0))
       ,Month8 = SUM(ISNULL(fbm.[8],0))
       ,Month9 = SUM(ISNULL(fbm.[9],0))
       ,Month10 = SUM(ISNULL(fbm.[10],0))
       ,Month11 = SUM(ISNULL(fbm.[11],0))
       ,Month12 = SUM(ISNULL(fbm.[12],0))
       ,Month13 = SUM(ISNULL(fbm.[13],0)) 
	   ,Month14 = SUM(ISNULL(fbm.[14],0))
       ,Month15 = SUM(ISNULL(fbm.[15],0))
       ,Month16 = SUM(ISNULL(fbm.[16],0))
       ,Month17 = SUM(ISNULL(fbm.[17],0))
       ,Month18 = SUM(ISNULL(fbm.[18],0))
       ,Month19 = SUM(ISNULL(fbm.[19],0))
       ,Month20 = SUM(ISNULL(fbm.[20],0))
       ,Month21 = SUM(ISNULL(fbm.[21],0))
       ,Month22 = SUM(ISNULL(fbm.[22],0))
       ,Month23 = SUM(ISNULL(fbm.[23],0)) 
	   ,Month24 = SUM(ISNULL(fbm.[24],0))
       ,Month25 = SUM(ISNULL(fbm.[25],0)) 
       ,Over25Mths = SUM(ISNULL(fbm.[26],0)) -- Over12Mths = SUM(ISNULL(fbm.[13],0))     
INTO #t
FROM    #FutureByMonth fbm
LEFT JOIN #Counts c
        ON c.CUSTOMER_ID = fbm.CUSTOMER_ID
		---AND c.KeyCustomer = fbm.KeyCustomer
		AND c.KeySourceSystem = fbm.KeySourceSystem
		AND c.SourceSystem = fbm.SourceSystem
GROUP BY  fbm.SourceSystem
       ,fbm.KeySourceSystem
       ,fbm.Client_Stream
       ,fbm.Client_ID
       ,fbm.Employee_ID
       ,fbm.CUSTOMER_ID
	   ,fbm.KeyCustomer
       ,fbm.LAST_NAME
       ,fbm.CurrBalance    
       ,fbm.STATUS_CODE
	   ,fbm.LIST_DATE
	   ,fbm.Commission
       ,c.TotalTaken
       ,c.PDC_Remaining
ORDER BY fbm.Client_Stream, fbm.Client_ID, fbm.CUSTOMER_ID;

/*
       ,fbm.Client_Stream
       
       ,fbm.Employee_ID
       ,fbm.CUSTOMER_ID
	   
       ,fbm.LAST_NAME
          
       ,fbm.STATUS_CODE
	   ,fbm.LIST_DATE
	   ,fbm.Commission
*/



---------------------------Insert data into table CLIENT_ANALYTICS.dbo.RPT_Postdate_25Mth_USBank------------------------------------------------------

TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Postdate_25Mth_USBank
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Postdate_25Mth_USBank
Select client_ID AS  [Client ID]
, Count(CUSTOMER_ID) AS #Accts
,SUM(CurrBalance) AS [Current Balance]
,SUM(TotalTaken) AS [#PostDates Taken]
,SUM(PDC_Remaining) AS [#PostDates Remaining]
,SUM(Month1) AS [Current Month $]
,SUM(Month2) AS [Next Month $]
,SUM(Month3) AS [Month 3 $]
,SUM(Month4) AS [Month 4 $]
,SUM(Month5) AS [Month 5 $]
,SUM(Month6) AS [Month 6 $]
,SUM(Month7) AS [Month 7 $]
,SUM(Month8) AS [Month 8 $]
,SUM(Month9) AS [Month 9 $]
,SUM(Month10) AS [Month 10 $]
,SUM(Month11) AS [Month 11 $]
,SUM(Month12) AS [Month 12 $]
,SUM(Month13) AS [Month 13 $]
,SUM(Month14) AS [Month 14 $]
,SUM(Month15) AS [Month 15 $]
,SUM(Month16) AS [Month 16 $]
,SUM(Month17) AS [Month 17 $]
,SUM(Month18) AS [Month 18 $]
,SUM(Month19) AS [Month 19 $]
,SUM(Month20) AS [Month 20 $]
,SUM(Month21) AS [Month 21 $]
,SUM(Month22) AS [Month 22 $]
,SUM(Month23) AS [Month 23 $]
,SUM(Month24) AS [Month 24 $]
,SUM(Month25) AS [Month 25 $]
,SUM(Over25Mths) AS [Sum of Over 25 Months $]
 from #t
where client_id in(
'UBN11', 'UBN12', 'UBN14', 'UBN18', 'UBN19', 'UBN21', 'UBN7', 'UBN8', 'UBN9'
                                                                        ,'UBN41', 'UBN48', 'UBN49'             ---- addition of 3 clientids as per ticket No.490976 
																		,'UBN22', 'UBN23')                     ---- addition of 2 clientids as per ticket No.724938 
GROUP BY Client_ID

GO