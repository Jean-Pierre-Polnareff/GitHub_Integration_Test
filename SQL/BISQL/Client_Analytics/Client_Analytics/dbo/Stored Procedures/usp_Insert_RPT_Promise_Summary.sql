USE [CLIENT_ANALYTICS]
GO


CREATE PROCEDURE [dbo].[usp_Insert_RPT_Promise_Summary]
AS
/*
Description: 
Summary of promises for joining batch inventory report

Author			Date		Description
Amod Ramugade 	03/23/2023	Initial creation 
*/

SET NOCOUNT ON;

IF OBJECT_ID('Tempdb..#t') IS NOT NULL
    DROP TABLE #t;
SELECT Batch_Mo
			   , KeySourceSystem
			   , CLIENT_ID
			   , CUSTOMER_STATE
			   , count(*) as promises
			   , count(distinct CUSTOMER_ID) as unq_promises
			   , sum(PROMISE_PAYMENT) as promise_amt
			   , sum(case when month(PROMISE_PAY_DATE)=month(getdate())
	                           and year(PROMISE_PAY_DATE)=year(getdate())
			              then PROMISE_PAYMENT
			              else 0
			              end) as promise_amt_currmo
			   , Charge_Off_Ind
			   , Balance_Ranges
	INTO #t 
FROM (
SELECT  
SourceSystem = 'FACS'
           ,KeySourceSystem = 4
	       ,ST.Client_Stream
		   ,ST.Parent
           ,CUST.EMPLOYEE_ID
           ,E.FIRST_NAME + ' ' + E.LAST_NAME AS Emp_Name
           ,D.DEPARTMENT_DESC
           ,D.DEPARTMENT_ID
           ,PD.CUSTOMER_ID
		   ,obf.INITIAL_BALANCE
           ,PD.CLIENT_ID
           ,CL.TRUST_ID
           ,CUST.LIST_DATE
           ,CUST.CUSTOMER_STATE
           ,PD.PROMISE_PAY_DATE
           ,DT1.MONTH_DATE AS Batch_Mo
           ,DT2.MONTH_DATE AS Post_date_Mo
           ,PD.PROMISE_PAYMENT AS PROMISE_PAYMENT
           ,Neu_Owner_ID = usb.Owner_ID 
           ,PD.IMPORT_DATE        
           , Charge_Off_Ind = CASE WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 0
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < .5
             THEN 'A - <6mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= .5
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 1
             THEN 'B -6mos-12mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 1
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 1.5
             THEN 'C -12mos-18mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 1.5
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 2
             THEN 'D -18mos-24mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 2
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 3
             THEN 'E -2yr-3yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 3
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 4
             THEN 'F -3yr-4yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 4
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 5
             THEN 'G -4yr-5yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 5
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 6
             THEN 'H -5yr-6yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 6
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 7
             THEN 'I -6yr-7yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 7
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 8
             THEN 'J -7yr-8yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 8
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 9
             THEN 'K -8yr-9yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 9
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 10
             THEN 'L -9yr-10yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 10
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 11
             THEN 'M -10yr-11yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 11
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 12
             THEN 'N -11yr-12yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 12
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 13
             THEN 'O -12yr-13yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 13
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 14
             THEN 'P -13yr-14yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 14
                  AND DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 15
             THEN 'Q -14yr-15yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 >= 15
             THEN 'R -15+ years'
             WHEN DATEDIFF(DAY,ISNULL(Cast(cust.CHARGE_OFF_DATE as date),'1/1/2040'),ISNULL(Cast(cust.LIST_DATE as date),'1/1/2050')) / 360.00 < 0
             THEN 'S -Missing Info'
        END
		, Balance_Ranges = CASE 
				WHEN obf.INITIAL_BALANCE >0 AND obf.INITIAL_BALANCE <500 THEN 'A-0-$499'
				WHEN obf.INITIAL_BALANCE >=500 AND obf.INITIAL_BALANCE <1000 THEN 'B-$500-$999'
				WHEN obf.INITIAL_BALANCE >=1000 AND obf.INITIAL_BALANCE <2500 THEN 'C-$1000-$2499'
				WHEN obf.INITIAL_BALANCE >=2500 AND obf.INITIAL_BALANCE <4999 THEN 'D-$2500-$4999'
				WHEN obf.INITIAL_BALANCE >=5000 THEN 'E-$5000+'
				END
	       ,ST.Commission 
    
	
	FROM    DW_MSTR_DM.dbo.TBL_Customer_PostDates PD
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_CUSTOMER CUST
            ON PD.CUSTOMER_ID = CUST.CUSTOMER_ID
	LEFT OUTER join DW_MSTR_DM.dbo.OUTSTANDING_BALANCE_FACT obf  
	        on CUST.CUSTOMER_ID=obf.CUSTOMER_ID
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_CLIENT CL
            ON CUST.CLIENT_ID = CL.CLIENT_ID
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT1
            ON CUST.LIST_DATE = DT1.CALNDR_DT
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT2
            ON PD.PROMISE_PAY_DATE = DT2.CALNDR_DT
    LEFT OUTER JOIN DW_MSTR_DM.dbo.TblClientStreams ST
            ON PD.CLIENT_ID = ST.Client_ID
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE E
            ON CUST.EMPLOYEE_ID = E.EMPLOYEE_ID
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DEPARTMENT D
            ON E.DEPARTMENT_ID = D.DEPARTMENT_ID
    LEFT JOIN DW_MSTR_DM.dbo.USBankRetail_Codes usb
			ON PD.CUSTOMER_ID = usb.ACCOUNT_NUM
  ---WHERE PD.PROMISE_PAY_DATE >= DATEADD(day, DATEDIFF(day, 0, GETDATE()), 0)
    
UNION

 SELECT     dcs.SourceSystem
	       ,fcpd.KeySourceSystem
           ,dcl.ClientStream
		   ,dcl.ClientParent
           ,dcs.EmployeeId
		   ,E.FullName 
           ----,E.FirstName + ' ' + E.LastName AS Emp_Name
           ,D.DepartmentDescription
           ,D.DepartmentId
           ,dcs.CustomerId
		   ,dcs.InitialBalance
		   ,dcl.ClientId
           ,TRUST_ID = NULL
           ,dcs.ListDate
           ,dcs.CustomerState
		   ,DD2.CalendarDate AS PromiseDueDate                           -----CONVERT(CHAR(10), CONVERT(datetime, CAST(KeyDate_PromiseDueDate AS VARCHAR)), 120)
           ,DD1.MONTHDATE AS Batch_Mo
           ,DD2.MONTHDATE AS Post_date_Mo
           ,fcpd.PromiseDueAmt AS PromiseDueAmt
           ,Neu_Owner_ID = NULL 
           ,fcpd.insertDate      
           , Charge_Off_Ind = CASE WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 0
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < .5
             THEN 'A - <6mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= .5
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 1
             THEN 'B -6mos-12mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 1
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 1.5
             THEN 'C -12mos-18mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 1.5
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 2
             THEN 'D -18mos-24mos'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 2
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 3
             THEN 'E -2yr-3yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 3
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 4
             THEN 'F -3yr-4yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 4
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 5
             THEN 'G -4yr-5yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 5
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 6
             THEN 'H -5yr-6yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 6
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 7
             THEN 'I -6yr-7yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 7
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 8
             THEN 'J -7yr-8yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 8
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 9
             THEN 'K -8yr-9yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 9
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 10
             THEN 'L -9yr-10yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 10
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 11
             THEN 'M -10yr-11yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 11
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 12
             THEN 'N -11yr-12yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 12
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 13
             THEN 'O -12yr-13yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 13
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 14
             THEN 'P -13yr-14yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 14
                  AND DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 15
             THEN 'Q -14yr-15yrs'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 >= 15
             THEN 'R -15+ years'
             WHEN DATEDIFF(DAY,ISNULL(Cast(dcs.CHARGEOFFDATE as date),'1/1/2040'),ISNULL(Cast(dcs.LISTDATE as date),'1/1/2050')) / 360.00 < 0
             THEN 'S -Missing Info'
        END
		, Balance_Ranges = CASE 
				WHEN dcs.INITIALBALANCE >0 AND dcs.INITIALBALANCE <500 THEN 'A-0-$499' 
				WHEN dcs.INITIALBALANCE >=500 AND dcs.INITIALBALANCE <1000 THEN 'B-$500-$999'
				WHEN dcs.INITIALBALANCE >=1000 AND dcs.INITIALBALANCE <2500 THEN 'C-$1000-$2499'
				WHEN dcs.INITIALBALANCE >=2500 AND dcs.INITIALBALANCE <4999 THEN 'D-$2500-$4999'
				WHEN dcs.INITIALBALANCE >=5000 THEN 'E-$5000+'
				END
	       ,dcl.Commission

 FROM DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) 
----LEFT OUTER JOIN  DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK)
----            ON fcpd.KeySourceSystem = dss.KeySourceSystem
LEFT OUTER JOIN DW_MSTR_DM.dbo.DimCustomer dcs (NOLOCK)
            ON fcpd.KeyCustomer = dcs.KeyCustomer
LEFT OUTER JOIN DW_MSTR_DM.dbo.DimClient dcl(NOLOCK)
            ON dcl.clientid = dcs.ClientId
			AND dcs.SourceSystem = dcl.SourceSystem
LEFT OUTER JOIN DW_MSTR_DM.dbo.DimDate dd1 (NOLOCK)
            ON dcs.ListDate = dd1.CalendarDate
LEFT OUTER JOIN DW_MSTR_DM.dbo.DimDate dd2 (NOLOCK)
            ON fcpd.KeyDate_PromiseDueDate = dd2.KeyDate
LEFT OUTER JOIN  DW_MSTR_DM.dbo.DimEmployee E (NOLOCK)
            ON dcs.EmployeeId = E.EmployeeId
			AND dcs.SourceSystem = E.SourceSystem
LEFT OUTER JOIN DW_MSTR_DM.dbo.DimDepartment D (NOLOCK)
            ON E.DepartmentId = D.DepartmentId
			AND E.SourceSystem = D.SourceSystem
			WHERE 
			--CONVERT(CHAR(10), CONVERT(datetime, CAST(KeyDate_PromiseDueDate AS VARCHAR)), 120) >= CAST(GETDATE() AS DATE)
			DD2.CalendarDate >= CAST(GETDATE() AS DATE)
		   AND 
		   fcpd.Date_Broken is null
		) cstmr
		GROUP BY   
		Batch_Mo
			   , KeySourceSystem
			   , CLIENT_ID 
			   , CUSTOMER_STATE 
			   , Charge_Off_Ind 
			   , Balance_Ranges
			   ;

TRUNCATE TABLE CLIENT_ANALYTICS.[dbo].[RPT_Promise_Summary]
INSERT INTO CLIENT_ANALYTICS.[dbo].[RPT_Promise_Summary] SELECT * FROM #t 
----SELECT TOP 0 * INTO CLIENT_ANALYTICS.dbo.RPT_Promise_Summary FROM #t 
GO


