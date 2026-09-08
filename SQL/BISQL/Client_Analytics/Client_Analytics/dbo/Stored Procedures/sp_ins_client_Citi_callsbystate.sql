

			CREATE PROCEDURE [dbo].[sp_ins_client_Citi_callsbystate] AS 


				DELETE FROM client_analytics.dbo.client_Citi_callsbystate
               
				WHERE CALL_DATE >=DATEADD(DAY,-15,CAST(GETDATE() AS DATE));

				INSERT INTO client_analytics.dbo.client_Citi_callsbystate

				SELECT 
				         Cust.customer_id
				       , C.CALL_DATE
				       , DT.MONTH_DATE
				       , Inbound = SUM( CASE WHEN c.CALL_TYPE='IN' THEN 1 ELSE 0 END)				   
                       , outbound =SUM(CASE WHEN c.CALL_TYPE <>'IN' THEN 1 ELSE 0 END )
				       , COUNT(*) AS calls
				       , SUM(c.IsAdjRPC) AS rpcs
				       , customer_state

				FROM    DW_MSTR_DM.dbo.CALL_HISTORY_FACT C (NOLOCK)
				           LEFT JOIN    
						DW_MSTR_DM.dbo.LU_CUSTOMER Cust (NOLOCK) ON C.CUSTOMER_ID = Cust.CUSTOMER_ID
				           JOIN    
						DW_MSTR_DM.dbo.LU_DATE DT  (NOLOCK) ON C.CALL_DATE = DT.CALNDR_DT
				           LEFT JOIN    
						DW_MSTR_DM.dbo.LU_DATE DT2 (NOLOCK) ON Cust.LIST_DATE = DT2.CALNDR_DT
				           LEFT OUTER JOIN 
						DW_MSTR_DM.dbo.TimeZoneByState T (NOLOCK) ON Cust.CUSTOMER_STATE = T.State_Abbr
				           LEFT OUTER JOIN 
						DW_MSTR_DM.dbo.TblClientStreams St (NOLOCK) ON C.CLIENT_ID = St.Client_ID
				           LEFT OUTER JOIN 
						DW_MSTR_DM.dbo.LU_EMPLOYEE E (NOLOCK) ON C.EMPLOYEE_ID = E.EMPLOYEE_ID

				WHERE C.CALL_DATE >=DATEADD(DAY,-15,CAST(GETDATE() AS DATE)) AND C.CLIENT_ID='CBP1'

				GROUP BY  
				         Cust.customer_id,
						 C.CALL_DATE,
						 DT.MONTH_DATE,
						 customer_state
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\aramugade]
    AS [dbo];




GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\aramugade]
    AS [dbo];




GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\mhuang]
    AS [dbo];


GO



GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [corp\ravijaykumar]
    AS [dbo];


GO



GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[sp_ins_client_Citi_callsbystate] TO [CORP\aughodake]
    AS [dbo];

