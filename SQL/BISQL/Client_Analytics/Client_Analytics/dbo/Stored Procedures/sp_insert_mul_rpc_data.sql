USE [CLIENT_ANALYTICS]
GO
/****** Object:  StoredProcedure [dbo].[sp_insert_mul_rpc_data]    Script Date: 6/4/2026 1:50:59 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


/* 
Object: sp_insert_mul_rpc_data

Description: Identify and insert multiple RPCs happened on the same day for non FACS into rpt_mulrpc_transctdata

Author			Date		Description
Amod Ramugade	05/25/2023	Created
*/
CREATE PROCEDURE [dbo].[sp_insert_mul_rpc_data]
@Call_Date DATE = NULL

AS


BEGIN
	SET NOCOUNT ON;




	DROP TABLE IF EXISTS #temp_fcc;	
	SELECT EOMONTH(CAST(fcc.CallStartTime as date)) as EMonth
	,fcc.[CallCenterName]
	, fcc.[DialedPhoneNumber] 
	, fcc.KeyCustomer
	,fcc.[CallSeconds]
	, fcc.IsInbound
	,CASE WHEN fcc.[IsInbound]=1 THEN 'Inbound' ELSE 'Outbound' 
	 END AS TransactionType
	,fcc.[IsRPC]
	,fcc.[SessionId]
	,CAST(fcc.CallStartTime as date) CallStartTime
	,fcc.DialedAreaCode
	INTO #temp_fcc
	FROM [DW_MSTR_DM].[dbo].[FactCustomerCall] fcc WITH (NOLOCK)
	WHERE fcc.KeyDate_CallDate BETWEEN CONVERT(VARCHAR ,CAST(dateadd(dd,-31,isnull(@Call_Date,GETDATE())) AS DATE),112) 
									AND CONVERT(VARCHAR,CAST(dateadd(dd,0,isnull(@Call_Date,GETDATE())) AS DATE),112) 
	and fcc.[IsRPC]=1 
	and CAST(fcc.[CallStartTime] AS date) > dateadd(dd,-31,isnull(@Call_Date,GETDATE())) 
					and CAST(fcc.[CallStartTime] AS date) < isnull(@Call_Date,GETDATE())
	and	fcc.[CallCenterName] NOT Like '%Interactions%'
	;

	DROP TABLE IF EXISTS #temp_rc;
	SELECT rc.Phone_Number 
		,rc.Livevox_Result
	,rc.LV_Client_Name
	, rc.Session_Id
	INTO #temp_rc
	FROM DW_MSTR_DM.dbo.RadiusCall rc WITH (NOLOCK)
		WHERE rc.Call_Date BETWEEN CAST(dateadd(dd,-31,isnull(@Call_Date,GETDATE())) AS DATE)
									AND CAST(dateadd(dd,0,isnull(@Call_Date,GETDATE())) AS DATE)
	and rc.[Is_RPC]=1 
	and rc.Call_Date > dateadd(dd,-31,isnull(@Call_Date,GETDATE())) 
					and rc.Call_Date < isnull(@Call_Date,GETDATE())
	and	rc.[Call_Center_Name] NOT Like '%Interactions%'
;

DROP TABLE IF EXISTS #Results;
SELECT EOMONTH(CAST(fcc.CallStartTime as date)) as EMonth
	,fcc.[CallCenterName]
	,CASE WHEN fcc.[IsInbound]=1 THEN rc.Phone_Number 
	 ELSE fcc.[DialedPhoneNumber] 
	 END AS [DialedPhoneNumber]
	,fcc.[CallSeconds]
	,fcc.TransactionType
	,fcc.[IsRPC]
	,fcc.[SessionId]
	,cust.CustomerId
	,cust.ClientId
	,dcl.ClientParent
	,cust.SourceSystem
	,CAST(fcc.CallStartTime as date) CallStartTime
	,rc.Livevox_Result
	,rc.LV_Client_Name
	,fcc.DialedAreaCode
	INTO #Results 	
	FROM #temp_fcc fcc
			  inner join
		 #temp_rc rc  on fcc.SessionId = rc.Session_Id
			  inner join 
		 DW_MSTR_DM.dbo.DimCustomer cust (NOLOCK)ON fcc.KeyCustomer = cust.KeyCustomer and cust.StatusCode<>'DW_deactivate'
		      left  join
		 DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) on cust.ClientId=dcl.ClientId and cust.SourceSystem=dcl.SourceSystem
;
	-----------------------------------------------Extracting the RPC Data for last 31 days-----------------------------------------------
/*
	IF OBJECT_ID('tempdb..#Results') IS NOT NULL 
	DROP TABLE #Results
	
	SELECT EOMONTH(CAST(a.CallStartTime as date)) as EMonth
	,A.[CallCenterName]
	,CASE WHEN A.[IsInbound]=1 THEN rc.Phone_Number 
	 ELSE A.[DialedPhoneNumber] 
	 END AS [DialedPhoneNumber]
	,A.[CallSeconds]
	,CASE WHEN A.[IsInbound]=1 THEN 'Inbound' ELSE 'Outbound' 
	 END AS TransactionType
	,A.[IsRPC]
	,A.[SessionId]
	,dcu.CustomerId
	,dcu.ClientId
	,dcl.ClientParent
	,dcu.SourceSystem
	,CAST(a.CallStartTime as date) CallStartTime
	,rc.Livevox_Result
	,rc.LV_Client_Name
	,A.DialedAreaCode
	INTO dbo.#Results
	FROM [DW_MSTR_DM].[dbo].[FactCustomerCall] A WITH (NOLOCK)
		JOIN DW_MSTR_DM.dbo.RadiusCall rc WITH (NOLOCK) ON rc.session_id = A.sessionid 
		INNER JOIN DW_MSTR_DM.dbo.DimCustomer dcu WITH (NOLOCK) ON a.[KeyCustomer]=dcu.KeyCustomer 
						AND  dcu.StatusCode<>'DW_deactivate'
		LEFT JOIN DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK) ON dcu.ClientId=dcl.ClientId 
						AND dcu.sourcesystem=dcl.sourcesystem
	WHERE a.KeyDate_CallDate BETWEEN CONVERT(VARCHAR ,CAST(dateadd(dd,-31,isnull(@Call_Date,GETDATE())) AS DATE),112) 
									AND CONVERT(VARCHAR,CAST(dateadd(dd,0,isnull(@Call_Date,GETDATE())) AS DATE),112) 
	and [IsRPC]=1 
	and CAST([CallStartTime] AS date) > dateadd(dd,-31,isnull(@Call_Date,GETDATE())) 
					and CAST([CallStartTime] AS date) < isnull(@Call_Date,GETDATE())
	and	[CallCenterName] NOT Like '%Interactions%'
*/

	IF OBJECT_ID('tempdb..#t') IS NOT NULL 
	DROP TABLE #t;
	SELECT * into #t 
	FROM (SELECT * , ROW_NUMBER() OVER (Partition By SessionId order by DialedPhonenumber) [Rank]  
	FROM #Results)  a 
	where a.[rank] =1 
	;

	--Extracting dialedphone with multiple RPC
	IF OBJECT_ID('tempdb..#Results_1') IS NOT NULL 
	DROP TABLE #Results_1;
	SELECT CallStartTime
	,DialedPhoneNumber
	,SUM(IsRPC) as repcnt
	INTO dbo.#Results_1
	FROM dbo.#t
	GROUP BY CallStartTime
	,DialedPhoneNumber
	Having SUM(IsRPC)>1
	;

	---------Transactional data for multiple RPC
	IF OBJECT_ID('tempdb..#Results_2') IS NOT NULL 
	DROP TABLE #Results_2;
	SELECT DISTINCT 
	 A.EMonth
	,A.CallCenterName
	,A.DialedPhoneNumber
	,A.CallSeconds
	,A.TransactionType
	,A.IsRPC
	,A.SessionId
	,A.Livevox_Result
	,A.CustomerId
	,A.ClientId
	,A.ClientParent
	,A.SourceSystem
	,A.CallStartTime
	,A.LV_Client_Name
	,A.DialedAreaCode
	INTO dbo.#Results_2
	FROM dbo.#t a 
	inner join dbo.#Results_1 b 
	on a.DialedPhoneNumber=b.DialedPhoneNumber
	and a.CallStartTime=b.CallStartTime
;

	
	IF OBJECT_ID('tempdb..#Results_3') IS NOT NULL 
	DROP TABLE #Results_3;
	SELECT 
	 EMonth
	,CallCenterName
	,DialedPhoneNumber
	,CallSeconds
	,TransactionType
	,IsRPC
	,SessionId
	,Livevox_Result
	,CustomerId
	,ClientId
	,ClientParent
	,SourceSystem
	,CallStartTime
	,LV_Client_Name
	,CASE WHEN RNK=1 THEN 'First' else 'Nth' 
	 end as AttemptType 
	,DialedAreaCode
	 into dbo.#Results_3
	  from (SELECT *, ROW_NUMBER() over(partition by dialedphonenumber order by callstarttime) rnk 
		FROM #Results_2) a ;


	------------------------------------Insert Calls Exceptions into CLIENT_ANALYTICS.dbo.rpt_mulrpc_transctdata_new-----------------------
	INSERT INTO CLIENT_ANALYTICS.dbo.rpt_mulrpc_transctdata_new
	(EMonth,CallCenterName,DialedPhoneNumber,
	CallSeconds,TransactionType,IsRPC,SessionId,
	Livevox_Result,CustomerId,ClientId,ClientParent,
	SourceSystem,CallStartTime,LV_Client_Name,DialedAreaCode, AttemptType, Insert_Date, call_history_fact_id)
	SELECT 
	 exc.EMonth
	,exc.CallCenterName
	,exc.DialedPhoneNumber
	,exc.CallSeconds
	,exc.TransactionType
	,exc.IsRPC
	,exc.SessionId
	,exc.Livevox_Result
	,exc.CustomerId
	,exc.ClientId
	,exc.ClientParent
	,exc.SourceSystem
	,exc.CallStartTime
	,exc.LV_Client_Name
	,exc.DialedAreaCode
	,exc.AttemptType
	,Insert_Date =  Getdate()
	, call_history_fact_id = NULL
	FROM dbo.#Results_3 exc

	LEFT OUTER JOIN
			CLIENT_ANALYTICS.dbo.rpt_mulrpc_transctdata_new rmt WITH (NOLOCK) 
			ON exc.SessionId=rmt.SessionId   
		
	WHERE rmt.sessionid IS NULL 
			AND rmt.call_history_fact_id IS NULL   

END;
                   
GO


