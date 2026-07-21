USE [CLIENT_ANALYTICS]
GO


ALTER PROCEDURE [dbo].[usp_RPT_TCPA_30_Day_Excpt]  

AS 
BEGIN

	SET NOCOUNT ON; 

	DROP TABLE IF EXISTS #temp_fcc_automated 

	SELECT fcc.[KeyCustomerCall],[SessionId],fcc.[KeyCustomer],fcc.[KeyClient],fcc.[KeySourceSystem],[KeyDate_CallDate],[KeyEmployee],
		[CallCenterName],[CallStartTime],[CallStartHour],[DialedPhoneNumber],
		[CallSeconds],[TalkSeconds],[HoldSeconds],[IsInbound],[IsOutbound],[IsConnect],[IsManual],[IsHCI],[IsRPC],[IsPromise],[IsDialer],
		fcc.[KeyDialerResult],fcc.[KeyDialerServiceType],[DialedAreaCode],[PhonePosition],fcc.[Consumer_ID],fcc.[zip_scf],fcc.[zip], 
		c.CustomerId, c.ClientId, cl.ClientParent, ss.SourceSystem,  
		rc.lv_client_name, rc.livevox_result, rc.result_id, rc.Call_Date, fcc.Dialer, fcc.Dialer_Result, tt.Transaction_Type 
	INTO #temp_fcc_automated 
	FROM (SELECT keycustomercall, sessionid, keycustomer, keyclient, keysourcesystem, keydate_calldate, keyemployee, fcc.[KeyDialerResult],fcc.[KeyDialerServiceType],KeyDialerTransactionType, [Consumer_ID], zip_scf, zip, 
						[CallCenterName],[CallStartTime],[CallStartHour],[DialedPhoneNumber],[DialedAreaCode],[PhonePosition],
						[CallSeconds],[TalkSeconds],[HoldSeconds],[IsInbound],[IsOutbound],[IsConnect],[IsManual],[IsHCI],[IsRPC],[IsPromise],[IsDialer],r.Dialer_Result, r.Dialer 
			from DW_MSTR_DM.dbo.FactCustomerCall fcc  with (nolock) 
				join DW_MSTR_DM.dbo.DimDialerServiceType  st with (nolock) on st.KeyDialerServiceType = fcc.KeyDialerServiceType 
				join DW_MSTR_DM.dbo.DimDialerResult r with (nolock) on r.keydialerresult = fcc.keydialerresult 
			where KeyDate_CallDate >= convert(varchar,dateadd(dd,-60,cast(getdate() as date)), 112) 
				and (st.service_type like '%UA%'  
					or fcc.CallCenterName like '%interactions%') 
				and r.Dialer_Result not like '%SMS%'  
				and r.Dialer_Result not like '%Text%' 
				and r.Dialer_Result != 'No Answer') fcc 
		 join DW_MSTR_DM.dbo.RadiusCall rc with (nolock, forceseek) on rc.session_id = fcc.sessionid 
		 join DW_MSTR_DM.dbo.DimCustomer c with (nolock) on c.KeyCustomer = fcc.KeyCustomer 
		 join DW_MSTR_DM.dbo.DimClient cl with (nolock) on cl.KeyClient = fcc.KeyClient  
		 join DW_MSTR_DM.dbo.DimDialerTransactionType tt with (nolock) on tt.KeyDialerTransactionType = fcc.KeyDialerTransactionType 
		 join DW_MSTR_DM.dbo.DimSourceSystem ss with (nolock) on ss.KeySourceSystem = fcc.KeySourceSystem 
	where fcc.IsOutbound = 1                  
  
	create index #tix_#temp_fcc_automated on #temp_fcc_automated (KeyCustomer,CallStarttime) 
  
	drop table if exists #temp_fcc_automated_lag  

	select lag(KeyCustomer, 1, null) over (order by KeyCustomer,CallStartTime) KeyCustomer_1, 
		lag(KeyCustomer, 2, null) over (order by KeyCustomer,CallStartTime) KeyCustomer_2, 
		lag(KeyCustomer, 3, null) over (order by KeyCustomer,CallStartTime) KeyCustomer_3,
		lag(CallStartTime, 1, null) over (order by KeyCustomer,CallStartTime) CallStartTime_1, 
		lag(CallStartTime, 2, null) over (order by KeyCustomer,CallStartTime) CallStartTime_2, 
		lag(CallStartTime, 3, null) over (order by KeyCustomer,CallStartTime) CallStartTime_3, 
		lag(SessionId, 1, 0) over (order by KeyCustomer,CallStartTime) SessionId_1, 
		lag(SessionId, 2, 0) over (order by KeyCustomer,CallStartTime) SessionId_2,  
		lag(SessionId, 3, 0) over (order by KeyCustomer,CallStartTime) SessionId_3,  
		* 
	into #temp_fcc_automated_lag 
	from #temp_fcc_automated 
	where CustomerId > 0 

	insert into [client_analytics].[dbo].[RPT_TCPA_excpt_auto_svc]	 
	(		
		  [dlr_excpt_id]
		  ,[keycustomercall]
		  ,[call_history_fact_id]
		  ,[customerid]
		  ,[clientid]
		  ,[sourcesystem]
		  ,[calldate]
		  ,[sessionid]
		  ,[insert_date]
		  ,[DialedPhoneNumber]
		  ,[CallStartTime]
		  ,[CallSeconds]
		  ,[ClientParent] 
		  ,[sessionid-1]
		  ,[CallStartTime-1]
		  ,[daydiff-1]
		  ,[sessionid-2]
		  ,[CallStartTime-2]
		  ,[daydiff-2]
		  ,[sessionid-3]
		  ,[CallStartTime-3]
		  ,[daydiff-3])
	select 
		30 dlr_excpt_id, 
		t.KeyCustomerCall, 
		NULL call_history_fact, 
		t.customerid,
		t.clientid, 
		t.sourcesystem,
		t.call_date calldate,
		t.sessionid, 
		getdate() insert_date,
		t.DialedPhoneNumber,
		t.CallStartTime, 
		t.CallSeconds,
		t.ClientParent, 
		--t.KeyCustomer, 
		--t.KeyCustomer_1,
		t.sessionid_1,
		t.CallStartTime_1, 
		datediff(dd,t.CallStartTime,t.CallStartTime_1) [datediff-1], 
		--KeyCustomer_2, 
		t.sessionid_2,
		t.CallStartTime_2, 
		datediff(dd,t.CallStartTime,t.CallStartTime_2) [datediff-2],
		--t.KeyCustomer_3,
		t.sessionid_3,
		t.CallStartTime_3, 
		datediff(dd,t.CallStartTime,t.CallStartTime_3) [datediff-3] 
	from #temp_fcc_automated_lag t 
		left join [client_analytics].[dbo].[RPT_TCPA_excpt_auto_svc] r on r.customerid = t.CustomerId 
																			and r.clientid = t.ClientId 
																			and r.sessionid = t.SessionId 
	where KeyCustomer = KeyCustomer_1 
		and KeyCustomer = KeyCustomer_2 
		and KeyCustomer = KeyCustomer_3  
		and datediff(dd,CallStartTime_1,t.CallStartTime) < 30 
		and datediff(dd,CallStartTime_2,t.CallStartTime) < 30 
		and datediff(dd,CallStartTime_3,t.CallStartTime) < 30 
		and r.keycustomercall is null 


END;

GO


