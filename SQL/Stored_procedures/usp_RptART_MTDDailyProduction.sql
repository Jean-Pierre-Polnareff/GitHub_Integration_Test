USE [DW_MSTR_DM]
GO

/****** Object:  StoredProcedure [dbo].[usp_RptART_MTDDailyProduction]    Script Date: 4/16/2026 8:22:32 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[usp_RptART_MTDDailyProduction]
	@FirstOfMonth DATETIME = NULL
AS
/*
Object: dbo.usp_RptART_MTDDailyProduction

Description:  Populates table dbo.TblRptART_MTDDailyProduction which is used in report ART_MTDDailyProductionDashboard.xlsx.

Author			Date		Description
Lara Zuleger	02/29/2016	Created based on code provided by Todd
Lara Zuleger	03/04/2016	Leverage new fields IsConnect and IsAdjRPC
Heidi Oldham	12/18/2017	Added nolocks to see if it would improve performance
Heidi Oldham	01/8/2019	Added promise amt, promist kept amt and promise kept ct, outbound ct and outbound rpct ct requested by Vauny Chandara for Flagship reporting. 
							Applied Template.

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
		, @CurrentDateTime DATETIME			--transaction time stamp
		, @StartDate DATETIME				--first of month (or month passed as @FirstOfMonth) 
		, @EndDate DATETIME					--first of next month
		, @RecordCountMessage VARCHAR(512)  --record count message

--Initialize variables
	SELECT 
	 @SpName = OBJECT_NAME(@@PROCID)
	,@Sp_ReturnCode = 0
	,@Step = N'Step 0: variable initialized'
	,@CurrentDateTime = GETDATE()
	,@StartDate = ISNULL(@FirstOfMonth,DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()-1), 0))
	,@EndDate = DATEADD(m, DATEDIFF(m, -1, @StartDate), 0)
	,@RecordCountMessage = '';

	BEGIN TRY
	
		SET @Step = N'Step 1: Truncate and reload TblRptART_MTDDailyProduction';			
		TRUNCATE TABLE DW_MSTR_DM.dbo.TblRptART_MTDDailyProduction;

		INSERT INTO DW_MSTR_DM.dbo.TblRptART_MTDDailyProduction
		SELECT  
		 fct.CLIENT_ID
		,st.Client_Stream
		,fct.EMPLOYEE_ID
		,fct.THROWING_AGENT
		,emp.DEPARTMENT_ID
		,fct.CALL_DATE
		,List_Month = dt.MONTH_DATE 
		,fct.CGPOOL
		,Log_Ct = SUM(1)
		,Manual_Call_Ct = SUM(CASE WHEN fct.CALL_TYPE = 'MA' THEN 1 ELSE 0 END)
		,Manual_Connect_Ct = SUM(CASE WHEN fct.CALL_TYPE = 'MA' THEN fct.IsConnect ELSE 0 END) 
		,Dialer_Call_Ct = SUM(CASE WHEN fct.CALL_TYPE NOT IN ('MA','IN') THEN 1 ELSE 0 END)
		,Dialer_Connect_Ct = SUM(CASE WHEN fct.CALL_TYPE NOT IN ('MA','IN') THEN fct.IsConnect ELSE 0 END) 
		,Inbound_Call_Ct = SUM(CASE WHEN fct.CALL_TYPE = 'IN' THEN 1 ELSE 0 END)
		,Inbound_Connect_Ct = SUM(CASE WHEN fct.CALL_TYPE = 'IN' THEN fct.IsConnect ELSE 0 END) 
		,Ttl_RCode_RPC = SUM(CASE WHEN fct.CONTACT_CODE LIKE 'R%' THEN 1 ELSE 0 END)
		,Inbound_RCode_RPC = SUM(CASE WHEN fct.CALL_TYPE = 'IN' AND fct.CONTACT_CODE LIKE 'R%' THEN 1 ELSE 0 END)
		,Manual_RCode_RPC = SUM(CASE WHEN fct.CALL_TYPE = 'MA' AND fct.CONTACT_CODE LIKE 'R%' THEN 1 ELSE 0 END)
		,Dialer_RCode_RPC = SUM(CASE WHEN fct.CALL_TYPE NOT IN ('MA','IN') AND fct.CONTACT_CODE LIKE 'R%' THEN 1 ELSE 0 END)
		,Inbound_Adj_RPC_Ind = SUM(CASE WHEN fct.CALL_TYPE = 'IN' THEN fct.IsAdjRPC ELSE 0 END)
		,Manual_Adj_RPC_Ind = SUM(CASE WHEN fct.CALL_TYPE = 'MA' THEN fct.IsAdjRPC ELSE 0 END)
		,Dialer_Adj_RPC_Ind = SUM(CASE WHEN fct.CALL_TYPE NOT IN ('MA','IN') THEN fct.IsAdjRPC ELSE 0 END)
		,Inbound_Promise = SUM(CASE WHEN fct.CALL_TYPE = 'IN' AND fct.CONTACT_CODE = 'RPS' THEN 1 ELSE 0 END)
		,Manual_Promise = SUM(CASE WHEN fct.CALL_TYPE = 'MA' AND fct.CONTACT_CODE = 'RPS' THEN 1 ELSE 0 END)
		,Dialer_Promise = SUM(CASE WHEN fct.CALL_TYPE NOT IN ('MA','IN') AND fct.CONTACT_CODE = 'RPS' THEN 1 ELSE 0 END)	
		,Promise_Amt = SUM(CASE WHEN fct.CONTACT_CODE = 'RPS' THEN fct.CALL_PROMISE_AMT ELSE 0 END)		
		,Promise_Kept_Ct = ISNULL(SUM(fct.IsPromiseKept),0)
		,Promise_Kept_Amt = SUM(CASE WHEN fct.IsPromiseKept = 1 THEN fct.CALL_PROMISE_AMT ELSE 0 END)		
		FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)		
		JOIN DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK)
			ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
		JOIN DW_MSTR_DM.dbo.LU_DATE dt (NOLOCK)
			ON cust.LIST_DATE = dt.CALNDR_DT
		LEFT OUTER JOIN DW_MSTR_DM.dbo.TblClientStreams st (NOLOCK)
			ON cust.CLIENT_ID = st.Client_ID
		LEFT OUTER JOIN DW_MSTR_DM.dbo.LU_EMPLOYEE emp (NOLOCK)
			ON fct.EMPLOYEE_ID = emp.EMPLOYEE_ID
		WHERE CALL_DATE >= @StartDate AND CALL_DATE < @EndDate
		GROUP BY 
		 fct.CLIENT_ID
		,st.Client_Stream
		,fct.EMPLOYEE_ID
		,fct.THROWING_AGENT
		,emp.DEPARTMENT_ID
		,fct.CALL_DATE
		,dt.MONTH_DATE
		,fct.CGPOOL;

		SET @RecordCountMessage = @RecordCountMessage + CAST(@@ROWCOUNT AS VARCHAR)+ ' records inserted into DW_MSTR_DM.dbo.TblRptART_MTDDailyProduction. '  + CHAR(13)			
		
		SELECT @RecordCountMessage AS RecordCountMessage;		
		
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

