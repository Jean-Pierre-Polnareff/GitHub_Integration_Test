

/* 
Object: sp_insert_mul_rpc_data_facs

Description: Identify and insert multiple RPCs happened on the same day for FACS into rpt_mulrpc_transctdata
 
Amod Ramugade	05/25/2023	Created
*/
CREATE PROCEDURE [dbo].[sp_insert_mul_rpc_data_facs]
	
	@Call_Date DATE = NULL
	
	

AS
BEGIN
	SET NOCOUNT ON;

-----------------------------------------Extracting the RPC Data for the last 31 days ----------------------------------------------------------------------------------

	IF OBJECT_ID('tempdb..#Results') IS NOT NULL 
	DROP TABLE #Results
	SELECT EOMONTH(CAST(fct.Call_date as date),0) as EMonth
	,fct.CALL_HISTORY_FACT_ID
	--,A.[CallCenterName]
	,CASE WHEN fct.[Call_Type]= 'IN' THEN fct.CALL_IN_PHONE_NUMBER
	 ELSE fct.CALL_OUT_PHONE_NUMBER 
	 END AS [DialedPhoneNumber]
	,fct.CALL_DURATION_SECONDS as CallSeconds
	,CASE WHEN fct.[Call_Type]= 'IN' THEN 'Inbound' 
	 ELSE 'Outbound' 
	 END AS TransactionType
	,fct.isAdjRPC as IsRPC
	--,A.[SessionId]
	,fct.CUSTOMER_ID as customerid
	,fct.CLIENT_ID as clientid
	,tcs.Parent as ClientParent
	,'FACS' as SourceSystem
	,CAST(fct.CALL_DATE AS DATETIME) + CAST(fct.call_start_time as datetime) as callstarttime
	,fct.contact_code
	--,rc.LV_Client_Name
	,CASE WHEN fct.[Call_Type]= 'IN' THEN left(fct.CALL_IN_PHONE_NUMBER,3) 
	 ELSE left(fct.CALL_OUT_PHONE_NUMBER,3)  
	 END as DialedAreaCode
	INTO dbo.#Results
		FROM DW_MSTR_DM.dbo.CALL_HISTORY_FACT fct (NOLOCK)
			  inner join 
		 DW_MSTR_DM.dbo.LU_CUSTOMER cust (NOLOCK)ON fct.CUSTOMER_ID = cust.CUSTOMER_ID
		      inner join
		 DW_MSTR_DM.dbo.TblClientStreams tcs (NOLOCK) on cust.CLIENT_ID=tcs.Client_ID
	WHERE fct.CALL_DATE > dateadd(dd,-31,isnull(@Call_Date,GETDATE())) 
		  and fct.CUSTOMER_ID not in(0,12345)		--ignore missing account number recs
		  and ISNULL(fct.Data_Source,'') = 'NGLV'
;



IF OBJECT_ID('tempdb..#t') IS NOT NULL 
	DROP TABLE  #t
	SELECT * into #t 
	FROM (SELECT * , ROW_NUMBER() OVER (Partition By Call_history_FACT_ID order by DialedPhonenumber) [Rank]  
	FROM #Results)  a 
	where a.[rank] =1 
	


	--Extracting dialedphone with multiple RPC
	IF OBJECT_ID('tempdb..#Results_1') IS NOT NULL 
	DROP TABLE #Results_1
	SELECT CallStartTime
	,DialedPhoneNumber
	,SUM(IsRPC) as repcnt
	INTO dbo.#Results_1
	FROM dbo.#t
	GROUP BY CallStartTime
	,DialedPhoneNumber
	Having SUM(IsRPC)>1



	--Transactional data for multiple RPC
	IF OBJECT_ID('tempdb..#Results_2') IS NOT NULL 
	DROP TABLE #Results_2
	SELECT DISTINCT A.EMonth
	--,A.CallCenterName
	,A.DialedPhoneNumber
	,A.CallSeconds
	,A.TransactionType
	,A.IsRPC
	,A.CALL_HISTORY_FACT_ID
	--,A.Livevox_Result
	,A.CONTACT_CODE
	,A.CustomerId
	,A.ClientId
	,A.ClientParent
	,A.SourceSystem
	,A.CallStartTime
	--, A.LV_Client_Name
	,A.DialedAreaCode
	INTO dbo.#Results_2
	FROM dbo.#t a 
	inner join dbo.#Results_1 b 
	on a.DialedPhoneNumber=b.DialedPhoneNumber
	and a.CallStartTime=b.CallStartTime
	


	IF OBJECT_ID('tempdb..#Results_3') IS NOT NULL 
	DROP TABLE #Results_3
	SElect EMonth
	--,CallCenterName
	,DialedPhoneNumber
	,CallSeconds
	,TransactionType
	,IsRPC
	,CALL_HISTORY_FACT_ID
	--,Livevox_Result
	,CONTACT_CODE
	,CustomerId
	,ClientId
	,ClientParent
	,SourceSystem
	,CallStartTime
	--,LV_Client_Name
	,CASE WHEN RNK=1 THEN 'First' else 'Nth' 
	 end as AttemptType 
	,DialedAreaCode
	into dbo.#Results_3
	from (SELECT *, ROW_NUMBER() over(partition by dialedphonenumber order by callstarttime) rnk 
		  FROM #Results_2) a 
	

----------------------------------------------------------Insert to rpt_mulrpc_transctdata--------------------------------------------------------
	INSERT INTO CLIENT_ANALYTICS.dbo.rpt_mulrpc_transctdata_new
	(EMonth,CallCenterName,DialedPhoneNumber,
	CallSeconds,TransactionType,IsRPC,SessionId,
	Livevox_Result,CustomerId,ClientId,ClientParent,
	SourceSystem,CallStartTime,LV_Client_Name,DialedAreaCode, AttemptType,Insert_Date,CALL_HISTORY_FACT_ID)
	SELECT exc.EMonth
	,CallCenterName = NULL
	,exc.DialedPhoneNumber
	,exc.CallSeconds
	,exc.TransactionType
	,exc.IsRPC
	,SessionId = NULL
	,exc.CONTACT_CODE                                                   ---Livevox_Result
	,exc.CustomerId
	,exc.ClientId
	,exc.ClientParent
	,exc.SourceSystem
	,exc.CallStartTime
	,LV_Client_Name = 'Northland_Group'
	,exc.DialedAreaCode
	,exc.AttemptType
	,GetDate()
	,exc.CALL_HISTORY_FACT_ID
	FROM dbo.#Results_3 exc
			LEFT OUTER JOIN
		 CLIENT_ANALYTICS.dbo.rpt_mulrpc_transctdata_new rmt (NOLOCK) 
		 ON exc.CALL_HISTORY_FACT_ID=rmt.call_history_fact_id
	WHERE rmt.SessionId IS NULL 
		  AND rmt.call_history_fact_id IS NULL                            

END;