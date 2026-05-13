USE [DW_STAGING]
GO

/****** Object:  StoredProcedure [dbo].[WellsFargo_ins_WellsFargoMTDDashboard]    Script Date: 4/16/2026 8:24:09 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[WellsFargo_ins_WellsFargoMTDDashboard]
	@FirstOfMonth DATETIME = NULL
AS
/* 
Object: dbo.WellsFargo_ins_WellsFargoDailyDashboard

Description: Creates a Dataset to support the daily Wells Fargo MTD dashboard

History
Author			Date		Description
------------------------------------------------------
Heidi Oldham	07/28/2017	Created
Todd Sobiech	08/02/2017  Changed harded coded employee ids to department 305
Heidi Oldham	08/09/2017  Deduped the postdates in step 4
Heidi Oldham	12/19/2017  Exclude eboni boney and sadia ahmed from dashboard
Heidi Oldham	06/15/2018  Excluded 'TDH','GJB','RER','KNH' and added new clients WLF2 and WLF3
Heidi Oldham	07/03/2018  Added start date as an argument
Heidi Oldham	01/10/2018  Excluded employees NSG and ZSA
*/
BEGIN
	SET NOCOUNT ON;

--Declare local variables
    DECLARE 
          @SpName VARCHAR(60)				--the name of the stored proc
        , @ErrNum INT						--local variable for ERROR_NUMBER()
        , @ErrMsg VARCHAR(MAX)				--local variable for ERROR_MESSAGE()
        , @LineNum INT						--local variable for ERROR_LINE() 
        , @ErrorMessage VARCHAR(MAX)		--string containing error message
        , @Sp_ReturnCode INT				--local variable for return code after executing another proc
        , @Step VARCHAR(150)				--processing step message
        , @CurrentDate DATETIME				--current date
		, @StartDate DATETIME				--first day of the month for either the current or previous month (if it is the 1st)
		, @EndDate DATETIME					--last day of the month for either the current or previous month (if it is the 1st)

--Initialize variables
	SELECT 
	 @SpName = OBJECT_NAME(@@PROCID)
	,@Sp_ReturnCode = 0
	,@Step = N'Step 0: variable initialized'
	,@CurrentDate = GETDATE();

	--argument of month else start date of this month (unless it is the 1st of month, then use last month)
	SELECT @StartDate = CASE WHEN @FirstOfMonth IS NOT NULL THEN @FirstOfMonth			
			WHEN DAY(DATEADD(d,0,DATEDIFF(d,0,GETDATE()))) = 1 THEN DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE())-1, 0) 
			ELSE DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0) END 
    
    --first day of the next month
    SELECT @EndDate = DATEADD(MONTH, DATEDIFF(MONTH, 0, @StartDate)+1, 0)

	BEGIN TRY

		SET @Step = N'Step 1: Create temp table with month and client_id combinations';
		SELECT  
		mth.CALNDR_DT,
		tcs.EMPLOYEE_ID
		INTO #WFC_Agents
		FROM DW_MSTR_DM.dbo.LU_EMPLOYEE tcs (NOLOCK)
		CROSS JOIN 
		(
			SELECT DISTINCT CALNDR_DT
			FROM  DW_MSTR_DM.dbo.LU_DATE (NOLOCK)
			WHERE   CALNDR_DT >= @StartDate 
			AND CALNDR_DT < @EndDate) mth
	--		WHERE   CALNDR_DT >= '2017-07-01 00:00:00.000' 
	--		AND CALNDR_DT < '2017-08-01 00:00:00.000') mth
		WHERE tcs.DEPARTMENT_ID in ('305')   		       
		ORDER BY mth.CALNDR_DT ASC;

		SET @Step = N'Step 2: Create temp table with call data';
		SELECT 
		CHF.Employee_ID,
		Call_Date,
		SUM(CASE WHEN Call_Type not in ('MA','IN')THEN 1 ELSE 0 END) AS Dialer_Call_Ct,
		SUM(CASE WHEN Call_Type in ('MA')THEN 1 ELSE 0 END) AS Manual_Call_Ct,
		SUM(CASE WHEN Call_Type in ('IN') AND IsConsumerConnect = 1 THEN 1 ELSE 0 END) AS Inbound_Call_Ct,
		SUM(CASE WHEN IsAdjRPC = 1 THEN 1 ELSE 0 END) AS RPC,
		SUM(CASE WHEN Call_Type in ('IN') THEN CAST(INBOUND_TALK_TIME AS DECIMAL(18,2))ELSE 0 END) AS Inbound_Talk_Time,
		SUM(CASE WHEN Call_Type in ('MA') THEN MANUAL_TALK_TIME + AGENT_WAIT_TIME ELSE 0 END) AS Manual_Talk_Time,
		SUM(CASE WHEN Call_Type NOT in ('MA','IN') THEN CAST(AGENT_TALK_TIME AS DECIMAL(18,2))ELSE 0 END) AS Dialer_Talk_Time,  
		CAST(SUM(CASE WHEN Call_Type not in ('MA','IN')THEN ((CAST(AGENT_TALK_TIME AS DECIMAL(18,2))/60)/60) ELSE 0 END) AS DECIMAL(18,2)) AS Dialer_Talk_Time_Hrs,
		SUM(CASE WHEN CONTACT_CODE = 'RPS' or CALL_PROMISE_AMT >0 THEN 1 ELSE 0 END) as Promise_Ct,
		SUM(CALL_PROMISE_AMT) as Amt_Promises
		INTO #WFC_Calls
		FROM DW_MSTR_DM.dbo.Call_History_Fact (NOLOCK) CHF
		JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE LUE (NOLOCK)
			ON CHF.EMPLOYEE_ID = LUE.EMPLOYEE_ID

		JOIN DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK)
		    ON CHF.CLIENT_ID=tcs.Client_ID 

		WHERE Call_Date > = @StartDate AND Call_Date < @EndDate
		--WHERE Call_Date > = '2017-07-01' AND Call_Date < '2017-08-01'
		AND LUE.DEPARTMENT_ID IN ('305') 
		--AND CLIENT_ID in ('WLF1','WLF2','WLF3')
		AND tcs.Parent='Wells Fargo'
		GROUP BY
		CHF.EMPLOYEE_ID,
		Call_Date

		SET @Step = N'Step 3: Create temp table with payment data';
		SELECT 
		CREDITED_EMPLOYEE_ID, 
		Pymt_Date, 
		SUM(1) AS Pay_Ct,
		SUM(Payment_Amt_Applied) AS Collections
		INTO #WFC_Payments
		FROM DW_MSTR_DM.dbo.PAYMENT_FACT (NOLOCK) PAYF
		JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE LUE (NOLOCK)
			ON PAYF.CREDITED_EMPLOYEE_ID = LUE.EMPLOYEE_ID

			JOIN DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK)
		    ON PAYF.CLIENT_ID=tcs.Client_ID 

		WHERE PYMT_DATE >= @StartDate AND PYMT_DATE < @EndDate
		--WHERE PYMT_DATE > = '2017-07-01' AND PYMT_DATE < '2017-08-01'

		AND LUE.DEPARTMENT_ID IN ('305')  
		--AND CLIENT_ID IN ('WLF1','WLF2','WLF3') 
		AND tcs.Parent='Wells Fargo'
		AND PYMT_TYPE NOT IN ('DBJ','CRJ','PCK','CAN')
		GROUP BY CREDITED_EMPLOYEE_ID, Pymt_Date

		SET @Step = N'Step 4: Create temp table with postdates data and dedupe';
		SELECT 
		ROW_NUMBER() OVER(PARTITION BY CAST(PDH.Import_Date AS DATE), PDH.coll_id ORDER BY PDH.coll_id DESC) AS RowNumber,
		Coll_ID,
		CAST(PDH.Import_Date AS DATE) AS Import_Date,
		COUNT(Customer_ID) AS PostDateCt,
		SUM(PROMISE_PAYMENT) AS PostDatesAmt
		INTO #WFC_PDC
		FROM DW_MSTR_DM.dbo.TBL_Customer_PostDates_History PDH (NOLOCK)
		JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE LUE (NOLOCK)
			ON PDH.Coll_ID = LUE.EMPLOYEE_ID
		
		JOIN DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK)
		    ON PDH.CLIENT_ID=tcs.Client_ID 

		WHERE PDH.IMPORT_DATE >= @StartDate AND PDH.IMPORT_DATE < @EndDate
		--WHERE IMPORT_DATE > = '2017-07-01' AND IMPORT_DATE < '2017-08-01'
		AND LUE.DEPARTMENT_ID IN ('305')
		--AND Client_ID IN ('WLF1','WLF2','WLF3')
		AND tcs.Parent='Wells Fargo'
		GROUP BY Coll_ID, PDH.Import_Date
		
		DELETE FROM #WFC_PDC WHERE RowNumber = 2;

		SET @Step = N'Step 5: Truncate table and Insert final dataset';
		TRUNCATE TABLE DW_MSTR_DM.dbo.WellsFargoMTDDashboard
		
		INSERT INTO DW_MSTR_DM.dbo.WellsFargoMTDDashboard
		(			
		 Rpt_Date 
		,Coll_ID 
		,Coll_Name 
		,Dialer_Calls 
		,Manual_Calls 
		,Inbound_Calls
		,Total_Calls 
		,RPC 
		,System_Hours
		,Talk_Time_hrs 
		,Total_Dialer_Time_Hrs 
		,Dialer_Talk_Time_Hrs
		,Number_of_Promises 
		,Amount_of_Promises 
		,Today_Number_Payments 
		,Today_Collected 
		,Postdate_Number_Payments 
		,Postdates_Collected 
		,WFCCalls.Manual_Talk_time
		,WFCCalls.Inbound_Talk_time
		,WFCCalls.Dialer_Talk_time
		,Insert_Date
		)
		SELECT
		 A.CALNDR_DT AS Rpt_Date
		,A.Employee_id AS Coll_ID
		,EMP.FIRST_NAME + ' ' + Emp.LAST_NAME AS Coll_Name
		,ISNULL(WFCCalls.Dialer_Call_Ct,0) AS Dialer_Calls
		,ISNULL(WFCCalls.Manual_Call_Ct,0) AS Manual_Calls
		,ISNULL(WFCCalls.Inbound_Call_Ct,0) AS Inbound_Calls
		,ISNULL(WFCCalls.Dialer_Call_Ct,0) + ISNULL(WFCCalls.Manual_Call_Ct,0) + ISNULL(WFCCalls.Inbound_Call_Ct,0) AS Total_Calls
		,ISNULL(WFCCalls.RPC,0) AS RPC
		,ISNULL(TM.SystemMinutes,0)/60 AS System_Hours
		,((ISNULL(CAST(WFCCalls.Manual_Talk_time AS DECIMAL(18,2)) + CAST(WFCCalls.Inbound_Talk_time AS DECIMAL(18,2)) + CAST(WFCCalls.Dialer_Talk_time AS DECIMAL(18,2)),0))/60)/60 AS Talk_Time_hrs
		,((ISNULL(CAST(WFCCalls.Manual_Talk_time AS DECIMAL(18,2)),0))/60)/60 AS Total_Dialer_Time_hrs
		,WFCCalls.Dialer_Talk_Time_Hrs
		,ISNULL(WFCCalls.Promise_Ct,0) AS Number_of_Promises
		,ISNULL(WFCCalls.Amt_Promises,0) AS Amount_of_Promises
		,ISNULL(WFCPayments.Pay_Ct,0) AS Today_Number_Payments
		,ISNULL(WFCPayments.Collections,0) AS Today_Collected
		,ISNULL(WFCPostDate.PostDateCt,0) AS Postdate_Number_Payments
		,ISNULL(WFCPostDate.[PostDatesAmt],0) AS Postdates_Collected
		,ISNULL(WFCCalls.Manual_Talk_time,0) AS Manual_Talk_Time
		,ISNULL(WFCCalls.Inbound_Talk_time,0) AS Inbound_Talk_Time
		,ISNULL(WFCCalls.Dialer_Talk_time,0) AS Dialer_Talk_Time
		--,GETDATE() AS Insert_Date
		,@CurrentDate AS Insert_Date		
		FROM #WFC_Agents A
		LEFT JOIN #WFC_Calls WFCCalls 
			ON A.CALNDR_DT = WFCCalls.Call_Date AND A.Employee_id = WFCCalls.Employee_ID
		LEFT JOIN #WFC_Payments WFCPayments 
			ON A.CALNDR_DT = WFCPayments.PYMT_DATE AND A.Employee_id = WFCPayments.CREDITED_EMPLOYEE_ID
		LEFT JOIN #WFC_PDC WFCPostDate 
			ON A.CALNDR_DT = WFCPostDate.Import_Date AND A.Employee_id = WFCPostDate.Coll_ID
		LEFT JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE EMP (NOLOCK)
			ON A.Employee_id = EMP.EMPLOYEE_ID
		LEFT JOIN DW_MSTR_DM.dbo.UserTime TM (NOLOCK)
			ON A.CALNDR_DT = TM.WorkedDate
			AND A.EMPLOYEE_ID = TM.UserId 
		WHERE EMP.Employee_id NOT IN ('EGB','SAA','TDH','GJB','RER','KNH','NSG','ZSA','1SM')

	END TRY
	
	BEGIN CATCH
	
		SELECT 
		 @Sp_ReturnCode = -1
		,@ErrNum = ERROR_NUMBER()
		,@ErrMsg = ERROR_MESSAGE()
		,@LineNum = ERROR_LINE()
		,@ErrorMessage = N'Procedure failed at '+ @Step + N'LineNumber=' 
			+ CAST(@LineNum AS VARCHAR) + N',Error=' + CAST(@ErrNum AS VARCHAR) +
			N',ErrorMsg=' + @ErrMsg;
			
		SELECT  @ErrorMessage = @SpName + '::' + @ErrorMessage;
		
		RAISERROR(@ErrorMessage,16,1);
			
	END CATCH;
	 SET NOCOUNT OFF;
	 
	 RETURN(@Sp_ReturnCode); 
	
END;




GO

