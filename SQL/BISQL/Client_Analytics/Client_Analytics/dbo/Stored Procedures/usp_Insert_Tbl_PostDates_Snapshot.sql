USE [CLIENT_ANALYTICS]
GO

/****** Object:  StoredProcedure [dbo].[usp_Insert_Tbl_PostDates_Snapshot]    Script Date: 2/6/2023 1:46:07 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Object: dbo.usp_Insert_Tbl_PostDates_Snapshot
-- Create date: 02/06/2023

--  Description: 
--  For FACS, Inserts records from the first postdate import of the month into DW_MSTR_DM.dbo.Tbl_PostDates_Snapshot. 
--  Then, last 6 months Snapshot is fetched from this DW_MSTR_DM.dbo.Tbl_PostDates_Snapshot table and inserted into CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History].
--  For non-FACS, last 6 months Snapshot is fetched from DW_MSTR_DM.dbo.FactCustomerPostdate table and inserted into CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History].
--
-- 	History
-- 	Author		Date		Description
-- 	------------------------------------------------------
-- 	Amod Ramugade	02/06/2023	Created
-- ==================================================================
CREATE PROCEDURE [dbo].[usp_Insert_Tbl_PostDates_Snapshot]
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY

        DECLARE @ErrorMessage NVARCHAR(4000);
        DECLARE @ErrorSeverity INT;
        DECLARE @ErrorState INT;

        BEGIN TRAN;

        DECLARE @NextSnapshotMonth DATE
			,@LastPDCImportDate DATE;

        SET @NextSnapshotMonth = (SELECT DATEADD(MONTH,1,MAX(SnapshotMonth)) FROM DW_MSTR_DM.dbo.Tbl_PostDates_Snapshot);
        SET @LastPDCImportDate = (SELECT MAX(IMPORT_DATE) FROM DW_MSTR_DM.dbo.TBL_Customer_PostDates);

		--Check whether it's the first import of the month
        IF @LastPDCImportDate >= @NextSnapshotMonth 
		BEGIN
			/******************************************************************
				Insert current day's postdate file into Tbl_PostDates_Snapshot
			*******************************************************************/
			INSERT  INTO DW_MSTR_DM.dbo.Tbl_PostDates_Snapshot
					(SnapshotMonth
					,CUSTOMER_ID
					,PROMISE_PAY_DATE
					,PROMISE_PAYMENT
					,PAYMENT_TYPE
					,CLIENT_ID
					,LIST_DATE
					,Coll_ID
					,CUSTOMER_STATE
					,OriginalImportDate
					,ImportDate)
			SELECT  SnapshotMonth = CAST(ld.MONTH_DATE AS DATE)
					,pd.CUSTOMER_ID
					,PROMISE_PAY_DATE = CAST(pd.PROMISE_PAY_DATE AS DATE)
					,pd.PROMISE_PAYMENT
					,pd.PAYMENT_TYPE
					,pd.CLIENT_ID
					,LIST_DATE = CAST(pd.LIST_DATE AS DATE)
					,pd.Coll_ID
					,lc.CUSTOMER_STATE
					,pd.IMPORT_DATE
					,ImportDate = GETDATE()
			FROM    DW_MSTR_DM.dbo.TBL_Customer_PostDates pd
			JOIN    DW_MSTR_DM.dbo.LU_DATE ld
						ON ld.CALNDR_DT = CAST(pd.IMPORT_DATE AS DATE)
			LEFT JOIN DW_MSTR_DM.dbo.LU_CUSTOMER lc
						ON pd.CUSTOMER_ID = lc.CUSTOMER_ID;

		END; 

		DELETE FROM CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History]
		WHERE keysourcesystem = 4;

		INSERT INTO CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History]
		SELECT    SourceSystem = 'FACS'
				,KeySourceSystem = 4
				,ST.Client_Stream
				,ST.Parent
				,CL.TRUST_ID
				,CL.CLIENT_ID
				,PD.Coll_ID
				,Emp_Name = E.FIRST_NAME + ' ' + E.LAST_NAME
				,D.DEPARTMENT_DESC
				,D.DEPARTMENT_ID
				,PD.SnapshotMonth
				,Batch_Mo = DT1.MONTH_DATE
				,Post_date_Mo = ld.MONTH_DATE
				,PD.CUSTOMER_ID
				,PD.PROMISE_PAYMENT
				,PD.LIST_DATE
				,PD.CUSTOMER_STATE
				,PD.PROMISE_PAY_DATE
				,ST.Commission 
		FROM    DW_MSTR_DM.dbo.Tbl_PostDates_Snapshot PD
			LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_CLIENT CL
				ON PD.CLIENT_ID = CL.CLIENT_ID
			LEFT OUTER JOIN DW_MSTR_DM.dbo.TblClientStreams ST
				ON PD.CLIENT_ID = ST.Client_ID
			LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE DT1
				ON PD.LIST_DATE = DT1.CALNDR_DT
			LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DATE ld
				ON PD.PROMISE_PAY_DATE = ld.CALNDR_DT
			LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE E
				ON PD.Coll_ID = E.EMPLOYEE_ID
			LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_DEPARTMENT D
				ON E.DEPARTMENT_ID = D.DEPARTMENT_ID
		WHERE PD.SnapshotMonth >= DATEADD(MONTH,-6,GETDATE());

	
		-- SELECT 
		--          ,Neu_Owner_ID = usb.Owner_ID 
		--          ,PD.IMPORT_DATE          
		-- FROM    DW_MSTR_DM.dbo.TBL_Customer_PostDates PD
		--   LEFT JOIN DW_MSTR_DM.dbo.USBankRetail_Codes usb
		--		ON PD.CUSTOMER_ID = usb.ACCOUNT_NUM

		DECLARE @NextSnapshotMonth_nonFACS DATE
					,@LastSnapshotMonth_nonFACS DATE;

		SET @NextSnapshotMonth_nonFACS = (SELECT DATEADD(MONTH,1,MAX(SnapshotMonth)) FROM CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History]);
		SET @LastSnapshotMonth_nonFACS = (SELECT MAX(SnapshotMonth) FROM CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History]);

		--Check whether it's the first import of the month
		IF @LastSnapshotMonth_nonFACS >= @NextSnapshotMonth_nonFACS 
        BEGIN
			DELETE FROM CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History]
			WHERE keysourcesystem <> 4;

			DECLARE @cnt INT = -6;

			WHILE @cnt < 0
			BEGIN
				--IF OBJECT_ID('Tempdb..#t') IS NOT NULL
				--    DROP TABLE #t;
				INSERT INTO CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History]
				SELECT     dcs.SourceSystem
						,fcpd.KeySourceSystem
						,dcl.ClientStream
						,dcl.ClientParent
						,TRUST_ID = NULL
						,dcl.ClientId
						,dcs.EmployeeId
						,E.FullName 
						----,E.FirstName + ' ' + E.LastName AS Emp_Name
						,D.DepartmentDescription
						,D.DepartmentId
						,SnapshotMonth =  CAST(DATEADD(MONTH,@cnt,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)) AS DATE)
						,DD1.MONTHDATE AS Batch_Mo
						,DD2.MONTHDATE AS Post_date_Mo
						,dcs.CustomerId
						,fcpd.PromiseDueAmt AS PromiseDueAmt
						,dcs.ListDate
						,dcs.CustomerState
						,DD2.CalendarDate AS PromiseDueDate
						---,Neu_Owner_ID = NULL 
						---,fcpd.insertDate      
						---,CASE WHEN dcs.ChargeOffDate IS NOT NULL THEN 1 ELSE 0 END AS Charge_Off_Ind
						,dcl.Commission
				FROM DW_MSTR_DM.dbo.FactCustomerPostdate fcpd (NOLOCK) 
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
						---WHERE CONVERT(CHAR(10), CONVERT(datetime, CAST(KeyDate_PromiseDueDate AS VARCHAR)), 120) >= CAST(GETDATE() AS DATE)
						WHERE DD2.CalendarDate >= CAST(DATEADD(MONTH,@cnt,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)) AS DATE)
						AND 
						(
							fcpd.Date_Broken is null
							OR CAST(fcpd.Date_Broken AS DATE) >= CAST(DATEADD(MONTH,@cnt,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0)) AS DATE)
						)

				SET @cnt = @cnt + 1;
			END 
			------SELECT SnapshotMonth, count(*) FROM CLIENT_ANALYTICS.[dbo].[RPT_PostDates_History] GROUP BY SnapshotMonth
		END 

		COMMIT TRAN;

    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        SET @ErrorMessage = ERROR_MESSAGE();
        SET @ErrorSeverity = ERROR_SEVERITY();
        SET @ErrorState = ERROR_STATE();

        RAISERROR(@ErrorMessage,@ErrorSeverity,@ErrorState);
    END CATCH;
	
END;

GO
