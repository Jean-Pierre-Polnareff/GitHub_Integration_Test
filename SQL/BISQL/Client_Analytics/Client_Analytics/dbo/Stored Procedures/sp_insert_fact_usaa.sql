
CREATE  PROCEDURE [dbo].[sp_insert_fact_usaa]
@StartDateTime DATETIME = NULL

 as 
 BEGIN
	SET NOCOUNT ON;

-------------delete last 31 days data from table rpt_client_usaa_chf

delete from CLIENT_ANALYTICS.dbo.rpt_client_usaa_chf
where call_date > isnull(@StartDateTime,GETDATE())-31

-----------data for last 31 days
drop table if exists #ca_usaa

SELECT                  
                        'FACS' AS crm 
				       , C.CALL_DATE
				       , DT.MONTH_DATE		   
				       , C.CLIENT_ID
				       , St.Client_Stream
				       , st.Parent AS client_parent
				       , call_direction = CASE WHEN c.CALL_TYPE='IN' THEN 'Inbound'
											   ELSE 'Outbound'
											   END				   
				       , COUNT(*) AS calls
				       , SUM(c.IsConnect) AS connects
				       , SUM(c.IsAdjRPC) AS rpcs
				       , SUM(CASE WHEN C.CONTACT_CODE = 'RPS' THEN 1 ELSE 0 END) AS promises
				       , SUM(C.CALL_PROMISE_AMT) AS promise_call_amt			    
					   , 4 as KeySourceSystem
					   , RPC_Payment = CASE WHEN c.IsAdjRPC= 1 and c.IsPromise= 1 THEN 1
											   ELSE 0
											   END
					  , Insert_Date = getdate()

					   into #ca_usaa

				FROM    DW_MSTR_DM.dbo.CALL_HISTORY_FACT C (NOLOCK)			           
				           JOIN    
						DW_MSTR_DM.dbo.LU_DATE DT  (NOLOCK) ON C.CALL_DATE = DT.CALNDR_DT
				           left  JOIN 
						DW_MSTR_DM.dbo.TblClientStreams St (NOLOCK) ON C.CLIENT_ID = St.Client_ID
				where 
				   st.Parent = 'USAA' and
				   c.CALL_TYPE <> 'IN'
				   and c.call_date > isnull(@StartDateTime,GETDATE())-31
			    GROUP BY 
			       C.CALL_DATE
			     , DT.MONTH_DATE				 
			     , C.CLIENT_ID
			     , St.Client_Stream
			     , st.Parent				
			     , c.CALL_TYPE
			     , c.IsAdjRPC
			     , c.IsPromise

---------add last 31 days data into table rpt_client_usaa_chf

insert into CLIENT_ANALYTICS.dbo.rpt_client_usaa_chf 
select * from #ca_usaa


end