


CREATE PROCEDURE [dbo].sp_RPT_SMS_sendseq_summary
	
	
AS
/* 
Object: dbo.sp_RPT_SMS_sendseq_summary

Description: Build report summaries of SMS performance by send sequence

Author			Date		Description
Mike Campbell	02/28/2023	Created
*/

BEGIN
	SET NOCOUNT ON;

	        delete from client_analytics.dbo.RPT_SMS_sendseq_summary;

			drop table if exists #sms
			select fcc.keycustomer
				   , fcc.keysourcesystem
				   , dcl.ClientParent
				   , dcl.ClientStreamID
				   , dcl.ClientStream
				   , dcl.ClientSegmentationGroup
				   , dcl.KeyClient
				   , rc.Call_Date as sms_send_date
				   , dcu.CustomerId
				   , dcu.ClientId
				   , case when dcu.InitialBalance<250 then 'LT 250'
						  else 'GTEQ 250'
						  end as bal_group
				   , RANK () over(partition by fcc.keycustomer order by call_date) as rank_dt
			into #sms
			from dw_mstr_dm.dbo.RadiusCall rc (nolock)
					join
				 dw_mstr_dm.dbo.FactCustomerCall fcc (nolock) on rc.Session_Id=fcc.SessionId
					join
				 dw_mstr_dm.dbo.DimCustomer dcu (nolock) on fcc.keycustomer=dcu.KeyCustomer
					join
				 dw_mstr_dm.dbo.DimClient dcl (nolock) on dcu.keyclient=dcl.keyclient
			where rc.livevox_result in('SMS MT Delivered','SMS MT Failed')
				  and rc.Call_Date>='6/8/22'
				  and rc.call_date>=dateadd(YEAR,-1,getdate())

			create index kc on #sms(keycustomer)
			create index cid on #sms(customerid)
			create index clid on #sms(clientid)


			drop table if exists #stop
			select keycustomer
				   , min(min_stop_txt_dt) as min_optout_date
			into #stop
			from (
					select fcc.KeyCustomer
						   , min(rc.Call_Date) as min_stop_txt_dt
					from dw_mstr_dm.dbo.RadiusCall rc (nolock)
							join
						 dw_mstr_dm.dbo.FactCustomerCall fcc (nolock) on rc.Session_Id=fcc.SessionId
							join
						 dw_mstr_dm.dbo.DimCustomer dcu (nolock) on fcc.keycustomer=dcu.KeyCustomer
					where rc.Call_Date>='6/8/22'
						  and rc.Livevox_Result = 'Consumer responded Stop to text'
					group by fcc.KeyCustomer
					union
					select KeyCustomer
						   , min(sms_optout_date) as sms_optout_date
					from dw_mstr_dm.dbo.RadiusPhone (nolock)
					where SMS_optin_status='N'
						  and SMS_optout_date>='6/8/22'
					group by KeyCustomer
				 ) x
			group by KeyCustomer;


			--need to find
			---1)
			---for all stop/optout above, which is the sms send that is closest, but not after, that sms
			----then a distribution of which sms send in the sequence that sms was
			---2)
			---for all sms send, which had a payment following within 2 days
			----then a distribution of which sms send in the sequence that sms was
			----but in case of multiple payments, do we need something different?

			--1) How many sends until optout?
			drop table if exists #stop_distro;
			with stop_rank
			as
			(
			select s.KeyCustomer
				   , s.keysourcesystem
				   , s.ClientParent
				   , s.ClientStreamID
				   , s.ClientStream
				   , s.ClientSegmentationGroup
				   , s.ClientId
				   , s.KeyClient
				   , s.bal_group
				   , s.sms_send_date
				   , st.min_optout_date
				   , datediff(day,s.sms_send_date,st.min_optout_date) as days_sms_to_optout
				   , rank () over(partition by s.keycustomer order by datediff(day,s.sms_send_date,st.min_optout_date)) as rank_days
				   , s.rank_dt as sms_send_seq
			from #sms s
					join
				 #stop st on s.KeyCustomer=st.KeyCustomer
							 and s.sms_send_date<st.min_optout_date
					join
				 dw_mstr_dm.dbo.dimclient dcl (nolock) on s.ClientId=dcl.ClientId
			where s.KeyCustomer<>5678
				  and s.KeyCustomer<>2209343
			)

			select ClientId
				   , keysourcesystem
				   , ClientParent
				   , ClientStreamID
				   , ClientStream
				   , ClientSegmentationGroup
				   , KeyClient
				   , bal_group
				   , sms_send_seq
				   , count(*) as tally
			into #stop_distro
			from stop_rank
			where rank_days=1
			group by ClientId
				   , keysourcesystem
				   , ClientParent
				   , ClientStreamID
				   , ClientStream
				   , ClientSegmentationGroup
				   , KeyClient
				   , bal_group
					 , sms_send_seq



			--2) How many sends until payment
			drop table if exists #pay
			select s.KeyCustomer
				   , s.CustomerId
				   , s.ClientId
				   , s.sms_send_date
				   , min(pd.pymt_date) as min_pymt_date
			into #pay
			from #sms s
				   join
				 CLIENT_ANALYTICS.dbo.rpt_payment_detail pd on s.CustomerId=pd.CUSTOMER_ID
															   and s.ClientId=pd.CLIENT_ID
															   and datediff(day,s.sms_send_date,pd.pymt_date) between 0 and 2
			group by s.KeyCustomer
				   , s.CustomerId
				   , s.ClientId
					 , s.sms_send_date;

			drop table if exists #pay1
			select p.KeyCustomer
				   , p.sms_send_date
				   , p.min_pymt_date
				   , sum(pd.total_collections) as total_collections
			into #pay1
			from #pay p
				   join
				 CLIENT_ANALYTICS.dbo.rpt_payment_detail pd on p.CustomerId=pd.CUSTOMER_ID
															   and p.ClientId=pd.CLIENT_ID
															   and p.min_pymt_date=pd.pymt_date
			group by p.KeyCustomer
				   , p.sms_send_date
				   , p.min_pymt_date


			drop table if exists #pay_distro;
			with pay_rank
			as
			(
			select s.KeyCustomer
				   , s.keysourcesystem
				   , s.ClientParent
				   , s.ClientStreamID
				   , s.ClientStream
				   , s.ClientSegmentationGroup
				   , s.ClientId
				   , s.KeyClient
				   , s.bal_group
				   , s.sms_send_date
				   , p.min_pymt_date
				   , datediff(day,s.sms_send_date,p.min_pymt_date) as days_sms_to_pay
				   , rank () over(partition by s.keycustomer order by datediff(day,s.sms_send_date,p.min_pymt_date)) as rank_days
				   , s.rank_dt as sms_send_seq
				   , p.total_collections
			from #sms s
					join
				 #pay1 p on s.KeyCustomer=p.KeyCustomer
							 and s.sms_send_date<p.min_pymt_date
					join
				 dw_mstr_dm.dbo.DimClient dcl (nolock) on s.ClientId=dcl.clientid
			where s.KeyCustomer<>5678
				  and s.KeyCustomer<>2209343
			--order by 1,4	
			)

			select ClientId
				   , keysourcesystem
				   , ClientParent
				   , ClientStreamID
				   , ClientStream
				   , ClientSegmentationGroup
				   , KeyClient
				   , bal_group
				   , sms_send_seq
				   , count(*) as tally
				   , sum(total_collections) as total_collections
			into #pay_distro
			from pay_rank
			where rank_days=1
			group by ClientId
				   , keysourcesystem
				   , ClientParent
				   , ClientStreamID
				   , ClientStream
				   , ClientSegmentationGroup
				   , KeyClient
				   , bal_group
					 , sms_send_seq;



			with attempts
			as
			(
			select s.ClientId
				   , s.keysourcesystem
				   , s.ClientParent
				   , s.ClientStreamID
				   , s.ClientStream
				   , s.ClientSegmentationGroup
				   , s.KeyClient
				   , s.bal_group
				   , s.rank_dt
				   , count(*) as attempts
			from #sms s
				   join
				 dw_mstr_dm.dbo.DimClient dcl (nolock) on s.ClientId=dcl.ClientId
			group by s.ClientId
				   , s.keysourcesystem
				   , s.ClientParent
				   , s.ClientStreamID
				   , s.ClientStream
				   , s.ClientSegmentationGroup
				   , s.KeyClient
				   , s.bal_group
				   , s.rank_dt
			),

			cl_attempts
			as
			(
			select s.ClientId
				   , s.keysourcesystem
				   , s.ClientParent
				   , s.ClientStreamID
				   , s.ClientStream
				   , s.ClientSegmentationGroup
				   , s.KeyClient
				   , s.bal_group
				   , count(*) as attempts
			from #sms s
				   join
				 dw_mstr_dm.dbo.DimClient dcl (nolock) on s.ClientId=dcl.ClientId
			group by s.ClientId
				   , s.keysourcesystem
				   , s.ClientParent
				   , s.ClientStreamID
				   , s.ClientStream
				   , s.ClientSegmentationGroup
				   , s.KeyClient
				   , s.bal_group
			having count(*)>=100000
			)

			insert into client_analytics.dbo.RPT_SMS_sendseq_summary
			  (
			   ClientId
				   , keysourcesystem
				   , ClientParent
				   , ClientStreamID
				   , ClientStream
				   , ClientSegmentationGroup
				   , keyclient
				   , bal_group
				   , send_nbr
				   , attempts
				   , stops_after_this_attempt
				   , pays_after_this_attempt
				   , total_collections
			  )
			select a.ClientId
				   , a.keysourcesystem
				   , a.ClientParent
				   , a.ClientStreamID
				   , a.ClientStream
				   , a.ClientSegmentationGroup
				   , a.keyclient
				   , a.bal_group
				   , a.rank_dt as send_nbr
				   , a.attempts
				   , s.tally as stops_after_this_attempt
				   , p.tally as pays_after_this_attempt
				   , p.total_collections
			from attempts a
				   join
				 cl_attempts cla on a.ClientId=cla.ClientId
									and a.bal_group=cla.bal_group
									and a.keysourcesystem=cla.keysourcesystem
				   left join
				 #stop_distro s on a.rank_dt=s.sms_send_seq and a.ClientId=s.ClientId
															and a.bal_group=s.bal_group
															and a.keysourcesystem=s.keysourcesystem
				   left join
				 #pay_distro p on a.rank_dt=p.sms_send_seq and a.ClientId=p.ClientId
														   and a.bal_group=p.bal_group
														   and a.bal_group=p.bal_group
														   and a.keysourcesystem=p.keysourcesystem

END;