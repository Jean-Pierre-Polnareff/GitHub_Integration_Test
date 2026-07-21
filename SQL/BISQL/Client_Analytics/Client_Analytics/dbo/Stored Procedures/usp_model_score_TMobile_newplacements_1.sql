
CREATE   PROCEDURE [dbo].[usp_model_score_TMobile_newplacements]

AS
/* 
Object: dbo.usp_model_score_TMobile_newplacements

Description: Score incoming TMobile placements 

Author			Date		Description
Mei-chih Huang	08/22/2023	Created

*/
BEGIN
	SET NOCOUNT ON;

	IF EXISTS (SELECT 1 FROM Client_Analytics.dbo.TMobile_RADSCR_File_Share WHERE Process_Date IS NULL) 
	BEGIN 
		--build level lookup tables for Chargeoff Reason and Credit Class

		--Chargeoff Reason Level Lookup Table

		drop table if exists #chargeoff_reason_counts
		select Chrg_Off_Reasn
				, count(*) as num_accounts
				, sum(case when Pmt_Rcvd_Amt > 0 then 1 else 0 end) as num_paid		
		into #chargeoff_reason_counts
		from Client_Analytics.dbo.TMobile_RADSCR_File_Share 
		where Client_Analytics.dbo.TMobile_RADSCR_File_Share.Insert_Date >= DATEADD(day, -90, GETDATE())
		group by Chrg_Off_Reasn
		
		drop table if exists #chargeoff_reason_logs
		select Chrg_Off_Reasn
				, num_accounts
				, num_paid
				, case when num_accounts = 1 then 0 else log(num_accounts) end as log_numaccounts
				, case when (num_paid = 1 or num_paid = 0) then 0 else log(num_paid) end as log_numpaid					
		into #chargeoff_reason_logs
		from #chargeoff_reason_counts

		drop table if exists #chargeoff_reason_norm
		select Chrg_Off_Reasn
				, case when log_numpaid = 0 then 0 else log_numpaid / log_numaccounts end as norm_conversion
		into #chargeoff_reason_norm
		from #chargeoff_reason_logs

		drop table if exists #chargeoff_reason_lookup
		select Chrg_Off_Reasn
				, norm_conversion
				, NTILE(10) OVER(ORDER by norm_conversion DESC) as chargeoff_reason_level
		into #chargeoff_reason_lookup
		from #chargeoff_reason_norm


		--Credit Class Level Lookup Table

		drop table if exists #credit_class_counts
		select Credit_Class
				, count(*) as num_accounts
				, sum(case when Pmt_Rcvd_Amt > 0 then 1 else 0 end) as num_paid					
		into #credit_class_counts
		from Client_Analytics.dbo.TMobile_RADSCR_File_Share 
		where Client_Analytics.dbo.TMobile_RADSCR_File_Share.Insert_Date >= DATEADD(day, -90, GETDATE())
		group by Credit_Class  

		drop table if exists #credit_class_logs
		select Credit_Class
				, num_accounts
				, num_paid
				, case when num_accounts = 1 then 0 else log(num_accounts) end as log_numaccounts
				, case when (num_paid = 1 or num_paid = 0) then 0 else log(num_paid) end as log_numpaid					
		into #credit_class_logs
		from #credit_class_counts

		drop table if exists #credit_class_norm
		select Credit_Class
				, case when log_numpaid = 0 then 0 else log_numpaid / log_numaccounts end as norm_conversion
		into #credit_class_norm
		from #credit_class_logs

		drop table if exists #credit_class_lookup
		select Credit_Class
				, norm_conversion
				, NTILE(10) OVER(ORDER by norm_conversion DESC) as credit_class_level
		into #credit_class_lookup
		from #credit_class_norm

		
		--build table of new placements to score for the day

		IF OBJECT_ID('tempdb..#sample') IS NOT NULL
					DROP TABLE #sample;

		select Account 
			, Pmt_Rcvd_Amt
			, Insert_Date
			, Initial_Balance as InitialBalance
			, Start_Srv_Dte
			, Reciept_Dte
			, Charge_Off_Date
			, s.Chrg_Off_Reasn
			, cr.chargeoff_reason_level as chargeoffrsn_level_new
			, s.Credit_Class
			, cc.credit_class_level as creditclass_level_new
			, DATEDIFF(month,Start_Srv_Dte,Reciept_Dte) as age_acct_at_listdate
			, DATEDIFF(month,Start_Srv_Dte,Charge_Off_Date) as age_acct_at_chargeoff 

		into #sample
		from Client_Analytics.dbo.TMobile_RADSCR_File_Share (NOLOCK) s
				left join
			#chargeoff_reason_lookup cr on s.Chrg_Off_Reasn = cr.Chrg_Off_Reasn
				left join
			#credit_class_lookup cc on s.Credit_Class = cc.Credit_Class
		where Process_Date is null;


		--Score sample
		
		truncate table CLIENT_ANALYTICS.dbo.xgb_scores_tmobile

		Insert into CLIENT_ANALYTICS.dbo.xgb_scores_tmobile
		EXEC CLIENT_ANALYTICS.dbo.usp_model_score_tmobile @model = 'tmobile_model_v2', 
		 @q ='select *
		from #sample';


		--Save scored file

		drop table if exists #scored

		select s.Account
		, s.Insert_Date
		, t2.score 
		, NTILE(10) OVER(ORDER by score DESC) as ScoreGroup
		, CAST(getdate() as DATE) as Process_Date
		into #scored
		from #sample s
			left join 
			CLIENT_ANALYTICS.dbo.xgb_scores_tmobile t2 on s.Account=t2.ACCT_NUM;


		--Update table
	
		update tm
		set tm.Score = sc.score,
			tm.scoregroup = sc.ScoreGroup,
			tm.Process_Date = sc.Process_Date

		from Client_Analytics.dbo.TMobile_RADSCR_File_Share tm
				join
			 #scored sc on tm.Account=sc.Account
							and tm.Insert_Date = sc.Insert_Date;

	END 

END;