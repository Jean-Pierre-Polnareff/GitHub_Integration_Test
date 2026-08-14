USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[usp_Insert_RPT_POSTDATED_CHECKS]    Script Date: 2/8/2023 9:31:40 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO







-- =============================================
-- Object: dbo.usp_Insert_RPT_POSTDATED_CHECKS
-- Create date: 06/16/2022
--
--  Description: Provides all CRM data for Postdated Checks 
--
-- 	History
-- 	Author		       Date		Description
-- 	------------------------------------------------------
--	Amod Ramugade	06/16/2022	 Created
-- =============================================

CREATE PROCEDURE [dbo].[usp_Insert_RPT_POSTDATED_CHECKS] 

AS

SET NOCOUNT ON;

IF OBJECT_ID('Tempdb..#t') IS NOT NULL
    DROP TABLE #t;
    SELECT  SourceSystem = 'FACS'
           ,KeySourceSystem = 4
	       ,ST.Client_Stream
		   ,ST.Parent
           ,CUST.EMPLOYEE_ID
           ,E.FIRST_NAME + ' ' + E.LAST_NAME AS Emp_Name
           ,D.DEPARTMENT_DESC
           ,D.DEPARTMENT_ID
           ,PD.CUSTOMER_ID
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
           ,CASE WHEN cust.CHARGE_OFF_DATE IS NOT NULL THEN 1 ELSE 0 END AS Charge_Off_Ind
	       ,ST.Commission 
    
	INTO #t
	FROM    DW_MSTR_DM.dbo.TBL_Customer_PostDates PD
    LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_CUSTOMER CUST
            ON PD.CUSTOMER_ID = CUST.CUSTOMER_ID
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
           ,CASE WHEN dcs.ChargeOffDate IS NOT NULL THEN 1 ELSE 0 END AS Charge_Off_Ind
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
		   ;


TRUNCATE TABLE CLIENT_ANALYTICS.[dbo].[RPT_POSTDATED_CHECKS]
INSERT INTO CLIENT_ANALYTICS.[dbo].[RPT_POSTDATED_CHECKS] SELECT * FROM #t 
----SELECT TOP 0 * INTO CLIENT_ANALYTICS.dbo.RPT_POSTDATED_CHECKS FROM #t 

GO