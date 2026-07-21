CREATE PROCEDURE [dbo].[usp_RPT_Calls_Acct_Worked_Monthly_ins_curr_month]
		

AS


BEGIN
SET NOCOUNT ON;
         

	declare 
		@cal_dt	DATE =  DATEADD(month, DATEDIFF(month, 0, GETDATE()-1), 0) ;
		

                DELETE FROM CLIENT_ANALYTICS.dbo.RPT_Calls_Acct_Worked_Monthly 
				WHERE MONTH_DATE = @cal_dt;

							
				INSERT INTO  CLIENT_ANALYTICS.dbo.RPT_Calls_Acct_Worked_Monthly 
				(Month_Date,CRM,KeySourceSystem,CLIENT_ID,Accts_Worked) 
			
			SELECT
			 
			 DT.Month_Date
			,'FACS' AS CRM
			,KeySourceSystem = 4
			,CLIENT_ID
           ,Accts_Worked = COUNT(DISTINCT CUSTOMER_ID)
		   
		  FROM    DW_MSTR_DM.dbo.CALL_HISTORY_FACT (NOLOCK)
     JOIN  DW_MSTR_DM.dbo.LU_DATE DT 
	 on Call_Date = DT.CALNDR_DT
     WHERE  DT.Month_Date = @cal_dt	 
    GROUP BY CLIENT_ID
           ,DT.MONTH_DATE
		   
		   UNION

		   SELECT dd.MonthDate AS month_date
					     , dss.SourceSystem AS crm
						 , dss.KeySourceSystem
					     , dcu.ClientId AS client_id
						 ,Accts_Worked = COUNT(DISTINCT fcc.KeyCustomer)
						---,Accts_Worked = COUNT(DISTINCT dcu.CUSTOMERID)


				FROM DW_MSTR_DM.dbo.FactCustomerCall fcc (NOLOCK)
					   JOIN
					 DW_MSTR_DM.dbo.DimDate dd (NOLOCK) ON fcc.KeyDate_CallDate=dd.KeyDate
					   LEFT JOIN
					 DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK) ON fcc.KeyCustomer=dcu.KeyCustomer
					   JOIN
					 DW_MSTR_DM.dbo.DimSourceSystem dss (NOLOCK) ON fcc.KeySourceSystem=dss.KeySourceSystem
					   LEFT JOIN
					 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON dcu.ClientId=dcl.ClientId AND dcu.SourceSystem=dcl.SourceSystem
				  			 			   
				WHERE dd.MonthDate = @cal_dt
				GROUP BY dd.MonthDate
					    , dss.SourceSystem
						, dss.KeySourceSystem
					   , dcu.ClientId
					  

				


	


END;
GO





		  
		   