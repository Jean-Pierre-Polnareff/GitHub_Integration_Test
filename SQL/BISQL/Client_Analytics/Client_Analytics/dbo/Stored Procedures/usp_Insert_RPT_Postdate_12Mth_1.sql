



-- =============================================
-- Object: dbo.usp_Insert_RPT_Postdate_12Mth
-- Create date: 06/16/2022
--
--  Description: Provides all CRM data for PDCNext12Mth 
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	06/16/2022	 Created
-- =============================================

CREATE PROCEDURE [dbo].[usp_Insert_RPT_Postdate_12Mth] 

AS

SET NOCOUNT ON;

/***************************************************************************
	Create temp table to hold 13 months to be included in report
***************************************************************************/
IF OBJECT_ID('Tempdb..#Months') IS NOT NULL
    DROP TABLE #Months;

--Get 13 months to be reported in monthly fields
SELECT TOP 13 --12
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
       ,MthOrder = ISNULL(m.MonthOrder,14) ---ISNULL(m.MonthOrder,13)
       ,NumRemaining = COUNT(*) ---include all future pdc, not just next 12 months
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

	   UNION
-----------------------------------------For non-FACS-----------------------------------------
	   SELECT	   
	    dcs.SourceSystem
	   ,fcpd.KeySourceSystem
	   ,dcl.ClientStream
	   ,dcs.ClientId
	   ,dcs.EmployeeId
	   ,dcs.CustomerId
	   ,dcs.KeyCustomer
	   ,dcs.LastName
	   ,dcs.StatusCode
	   ,CurrBalance =ISNULL(dcs.CurrentBalance,0.00) 
       ,PostdateMonth = dd.MonthId
       ,MthOrder = ISNULL(m.MonthOrder,14) 
       ,NumRemaining = COUNT(*)                                 --include all future pdc, not just next 12 months
       ,MonthlyAmount = SUM(fcpd.PromiseDueAmt)  
	   ,dcs.ListDate
	   ,dcl.Commission

FROM    
         DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) 
JOIN    DW_MSTR_DM.dbo.DimCustomer dcs (NOLOCK)
        ON fcpd.KeyCustomer = dcs.KeyCustomer
JOIN	DW_MSTR_DM.dbo.DimClient dcl(NOLOCK)
ON dcl.clientid = dcs.ClientId
AND dcl.SourceSystem = dcs.sourcesystem
JOIN    DW_MSTR_DM.dbo.DimDate dd
        ON fcpd.KeyDate_PromiseDueDate = dd.KeyDate
LEFT JOIN #Months m
		ON dd.MonthId = m.Month_ID
WHERE 
--CONVERT(CHAR(10), CONVERT(datetime, CAST(fcpd.KeyDate_PromiseDueDate AS VARCHAR)), 120) >= CAST(GETDATE() AS DATE) 
DD.CalendarDate >= CAST(GETDATE() AS DATE) 
AND 
fcpd.Date_Broken is null
GROUP BY
        dcs.SourceSystem
	   ,fcpd.KeySourceSystem
       ,dcl.ClientStream
	   ,dcs.ClientId
       ,dcs.EmployeeId
       ,dcs.CustomerId
       ,dcs.KeyCustomer
       ,dcs.LastName
       ,dcs.StatusCode
       ,ISNULL(dcs.CurrentBalance,0.00) 
       ,dd.MonthId
       ,ISNULL(m.MonthOrder,14) 
	   ,dcs.ListDate
	   ,dcl.Commission
	   ;


----SELECT * FROM #PromiseByMonth

/*******************************************************
	Flatten out the 13 months and populate with monthly total
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
                                                                                [11],[12],[13],[14]) )AS PivotAmt;

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

UNION
-----------------------------------------For non-FACS-----------------------------------------
SELECT  fbm.sourcesystem , fbm.Keysourcesystem , fbm.CUSTOMER_ID , fbm.KeyCustomer
       ,NumPaid = COUNT(*)
FROM    (SELECT DISTINCT sourcesystem , Keysourcesystem, CUSTOMER_ID, KeyCustomer FROM #FutureByMonth where KeySourceSystem <> 4) fbm
JOIN    DW_MSTR_DM.dbo.FactCustomerPayment fcp (NOLOCK) 
ON fbm.KeyCustomer = fcp.KeyCustomer
LEFT JOIN DW_MSTR_DM.dbo.DimPaymentType dpt (NOLOCK) 
ON fcp.KeyPaymentType=dpt.KeyPaymentType       
WHERE  fcp.KeyDate_PaymentDate  >= '20160101'
	AND (dpt.PaymentCategory<>'Adjustment' OR dpt.PaymentCategory IS NULL)
			AND dpt.PaymentType NOT IN('DA','DAR')
--AND dpt.PaymentType NOT IN ('CAN','PCK','ADJ','DBJ','NSF','COR')
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
       ,Over13Mths = SUM(ISNULL(fbm.[14],0)) -- Over12Mths = SUM(ISNULL(fbm.[13],0))     
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


TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Postdate_12Mth
INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Postdate_12Mth SELECT * FROM #t 
---SELECT TOP 0 * INTO CLIENT_ANALYTICS.dbo.RPT_Postdate_12Mth FROM #t