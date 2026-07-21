USE [DW_MSTR_DM]
GO

/****** Object:  StoredProcedure [dbo].[usp_RPT_Model_Performance_Sloping_RptMonth_Auto]    Script Date: 8/28/2025 10:33:09 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





 


ALTER   PROCEDURE [dbo].[usp_RPT_Model_Performance_Sloping_RptMonth_Auto]

AS

/*
Description: 
Sloping for models

Author			Date		Description
Amod Ramugade   10/26/2023  Initial creation for Automotive model with Addition of ClientId, ClientParent, ClientStream, ClientStreamId 
                            and without the criteria of minimum 1000 accounts per Client as per ticket number
Amod Ramugade   01/10/2024  Removed the criteria of considering accounts which are at least 6 months old to include newly added accounts
*/

BEGIN
SET NOCOUNT ON;

  TRUNCATE TABLE CLIENT_ANALYTICS.dbo.RPT_Model_Performance_Sloping_RptMonth_Auto;

	--samp from 9/1/21 thru today
	 WITH total_vols
	 AS
	 (
	  SELECT dm.KeyModel
	         , dcl.ClientSegmentationGroup
			 , dcl.ClientStream
			 , dcl.ClientStreamId
			 , dcl.ClientParent 
			 , dcl.ClientId
			 , dss.SourceSystem
			 , SUM(mp.accounts) AS accounts
			 , SUM(mp.calls) AS calls
			 , SUM(mp.payments) AS payments
			 , CAST(SUM(mp.payments) AS FLOAT)/CAST(SUM(mp.accounts) AS FLOAT) AS yield
		FROM [CLIENT_ANALYTICS].[dbo].[RPT_Model_Performance_listmo] mp WITH (NOLOCK) 
				JOIN
			 DW_MSTR_DM.dbo.DimModel dm WITH (NOLOCK)  ON mp.KeyModel=dm.KeyModel
			    join
			 DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK)  on mp.ClientId=dcl.ClientId
                join
			 DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK)  on dcl.SourceSystem=dss.SourceSystem2
			                                       and mp.KeySourceSystem=dss.KeySourceSystem
		WHERE mp.list_month<= DATEADD(MONTH,0,GETDATE())                           ---mp.list_month<= DATEADD(MONTH,-6,GETDATE()) --samp from 9/1/21 thru 6 months ago
			  AND mp.list_month>='9/1/20'
			  AND LEN(mp.clientparent)>0
			  AND dm.ModelLongDescription = 'Automotive Segmentation'
		GROUP BY dm.KeyModel
		         , dcl.ClientSegmentationGroup
				 , dcl.ClientStream
			     , dcl.ClientStreamId
			     , dcl.ClientParent
			     , dcl.ClientId
				 , dss.SourceSystem
	 ),

	 score_vols
	 AS
	 (
	  SELECT dm.KeyModel
	         , dcl.ClientSegmentationGroup
			 , dcl.ClientStream
			 , dcl.ClientStreamId
			 , dcl.ClientParent
			 , dcl.ClientId
			 , dss.SourceSystem
			 , mp.ScoreGroup
			 , SUM(mp.accounts) AS accounts
			 , SUM(mp.calls) AS calls
			 , SUM(mp.payments) AS payments
			 , CAST(SUM(mp.payments) AS FLOAT)/CAST(SUM(mp.accounts) AS FLOAT) AS yield
		FROM [CLIENT_ANALYTICS].[dbo].[RPT_Model_Performance_listmo] mp WITH (NOLOCK) 
				JOIN
			 DW_MSTR_DM.dbo.DimModel dm WITH (NOLOCK)  ON mp.KeyModel=dm.KeyModel
			    join
			 DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK)  on mp.ClientId=dcl.ClientId
                join
			 DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK)  on dcl.SourceSystem=dss.SourceSystem2
			                                       and mp.KeySourceSystem=dss.KeySourceSystem
		WHERE mp.list_month<=DATEADD(MONTH,0,GETDATE())                            --mp.list_month<=DATEADD(MONTH,-6,GETDATE()) --samp from 9/1/21 thru 6 months ago
			  AND mp.list_month>='9/1/20'
			  AND LEN(mp.clientparent)>0
			  AND dm.ModelLongDescription = 'Automotive Segmentation'
		GROUP BY dm.KeyModel
		         , dcl.ClientSegmentationGroup
				 , dcl.ClientStream
			     , dcl.ClientStreamId
			     , dcl.ClientParent
			     , dcl.ClientId
				 , dss.SourceSystem
				 , mp.ScoreGroup
	 ),

	 currmo
	 AS
	 (
	  SELECT dm.KeyModel
	         , dm.ModelLongDescription
	         , dcl.ClientSegmentationGroup
			 , dcl.ClientStream
			 , dcl.ClientStreamId
			 , dcl.ClientParent
			 , dcl.ClientId
			 , dss.SourceSystem
			 , mp.rpt_month
			 , mp.ScoreGroup
			 , SUM(mp.accounts) AS accounts
			 , SUM(mp.calls) AS calls
			 , SUM(mp.payments) AS payments
			 , CAST(SUM(mp.payments) AS FLOAT)/CAST(SUM(mp.accounts) AS FLOAT) AS yield
		FROM --[CLIENT_ANALYTICS].[dbo].[RPT_Model_Performance_listmo] mp
		     CLIENT_ANALYTICS.dbo.RPT_Model_Performance mp WITH (NOLOCK) 
				JOIN
			 DW_MSTR_DM.dbo.DimModel dm WITH (NOLOCK)  ON mp.KeyModel=dm.KeyModel
			    join
			 DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK)  on mp.ClientId=dcl.ClientId
                join
			 DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK)  on dcl.SourceSystem=dss.SourceSystem2
			                                       and mp.KeySourceSystem=dss.KeySourceSystem
		WHERE mp.rpt_month>=DATEADD(MONTH,-3,GETDATE())
			  AND LEN(mp.clientparent)>0
			  AND dm.ModelLongDescription = 'Automotive Segmentation'
		GROUP BY dm.KeyModel
	         , dm.ModelLongDescription
		         , dcl.ClientSegmentationGroup
				 , dcl.ClientStream
			     , dcl.ClientStreamId
			     , dcl.ClientParent
			     , dcl.ClientId
				 , dss.SourceSystem
			     , mp.rpt_month
				 , mp.ScoreGroup
	 ),

	 currmo_tot
	 AS
	 (
	  SELECT dm.KeyModel
	         , mp.rpt_month
			 , dcl.ClientSegmentationGroup
			 , dcl.ClientStream
			 , dcl.ClientStreamId
			 , dcl.ClientParent
			 , dcl.ClientId
			 , dss.SourceSystem
			 , SUM(mp.accounts) AS accounts
			 , SUM(mp.calls) AS calls
			 , SUM(mp.payments) AS payments
			 , CAST(SUM(mp.payments) AS FLOAT)/CAST(SUM(mp.accounts) AS FLOAT) AS yield
		FROM --[CLIENT_ANALYTICS].[dbo].[RPT_Model_Performance_listmo] mp
		     CLIENT_ANALYTICS.dbo.RPT_Model_Performance mp WITH (NOLOCK) 
				JOIN
			 DW_MSTR_DM.dbo.DimModel dm WITH (NOLOCK)  ON mp.KeyModel=dm.KeyModel
			    join
			 DW_MSTR_DM.dbo.DimClient dcl WITH (NOLOCK)  on mp.ClientId=dcl.ClientId
                join
			 DW_MSTR_DM.dbo.DimSourceSystem dss WITH (NOLOCK)  on dcl.SourceSystem=dss.SourceSystem2
			                                       and mp.KeySourceSystem=dss.KeySourceSystem
		WHERE mp.rpt_month>=DATEADD(MONTH,-3,GETDATE())
			  AND LEN(mp.clientparent)>0
			  AND dm.ModelLongDescription = 'Automotive Segmentation'
		GROUP BY dm.KeyModel
	             , mp.rpt_month
		         , dcl.ClientSegmentationGroup
				 , dcl.ClientStream
			     , dcl.ClientStreamId
			     , dcl.ClientParent
			     , dcl.ClientId
				 , dss.SourceSystem
	 ),

	 yield_pie
	 AS
	 (
		SELECT sv.KeyModel
		       , sv.ClientSegmentationGroup
			   , sv.ClientStream
			   , sv.ClientStreamId
			   , sv.ClientParent
			   , sv.ClientId
			   , sv.ScoreGroup
			   , sv.SourceSystem
			   , case when tv.yield is null or tv.yield = 0 then 0 else sv.yield / tv.yield end AS yield_idx 
		FROM score_vols sv
			   JOIN
			 total_vols tv ON sv.ClientSegmentationGroup=tv.ClientSegmentationGroup
			                  AND sv.ClientStream	  = tv.ClientStream
			                  AND sv.ClientStreamId   = tv.ClientStreamId
			                  AND sv.ClientParent	  = tv.ClientParent
			                  AND sv.ClientId		  = tv.ClientId
			                  AND sv.KeyModel=tv.KeyModel
							  AND sv.SourceSystem     = tv.SourceSystem
	 ),

	 yield_tot
	 AS
	 (
		SELECT KeyModel
		      , ClientSegmentationGroup
			  , ClientStream
			  , ClientStreamId
			  , ClientParent
			  , ClientId
			  , SourceSystem
			   , SUM(yield_pie.yield_idx) AS yield_idx_tot
		FROM yield_pie
		GROUP BY KeyModel
		         , ClientSegmentationGroup
				 , ClientStream
			     , ClientStreamId
			     , ClientParent
			     , ClientId
				 , SourceSystem
	 )


	 --leaving some commented cols below for future quick reference on calculation of predicted_call_pct if needed
	 INSERT INTO CLIENT_ANALYTICS.dbo.RPT_Model_Performance_Sloping_RptMonth_Auto
	 (
	 [KeyModel]
      ,[ModelLongDescription]
      ,[ClientSegmentationGroup]
      ,[ClientStream]
      ,[ClientStreamId]
      ,[ClientParent]
      ,[ClientId]
      ,[rpt_month]
      ,[scoregroup]
      ,[accounts]
      ,[calls]
      ,[payments]
      ,[currmo_yield]
      ,[historic_segment_yield]
      ,[pct_currmo_calls]
      ,[predicted_call_pct]
	  ,[SourceSystem]
	  )
	 SELECT cm.KeyModel
	        , cm.ModelLongDescription
			, cm.ClientSegmentationGroup
			, cm.ClientStream
			, cm.ClientStreamId
			, cm.ClientParent
			, cm.ClientId
			, cm.rpt_month
			, CAST(cm.ScoreGroup AS INT) AS scoregroup
			, cm.accounts
			, cm.calls
			, cm.payments
			, CAST(cm.payments AS FLOAT)/CAST(cm.accounts AS FLOAT) AS currmo_yield
	--		, cm.yield
			, sv.yield AS historic_segment_yield
	--		, tv.yield
	--		, sv.yield/tv.yield AS yield_idx
	--		, yt.yield_idx_tot
			, CAST(cm.calls AS FLOAT)/CAST(ct.calls AS FLOAT) AS pct_currmo_calls
			, case when yt.yield_idx_tot is null or yt.yield_idx_tot = 0 then 0 else (case when tv.yield is null or tv.yield = 0 then 0 else sv.yield / tv.yield end) / yt.yield_idx_tot end AS predicted_call_p ct
			, cm.SourceSystem
	 FROM currmo cm
			LEFT JOIN
		  score_vols sv ON cm.ClientSegmentationGroup=sv.ClientSegmentationGroup
		  			          AND cm.ClientStream	  = sv.ClientStream
			                  AND cm.ClientStreamId   = sv.ClientStreamId
			                  AND cm.ClientParent	  = sv.ClientParent
			                  AND cm.ClientId		  = sv.ClientId
							  AND cm.SourceSystem	  = sv.SourceSystem
						   AND cm.ScoreGroup=sv.ScoreGroup
						   AND cm.KeyModel=sv.KeyModel
			LEFT JOIN
		  total_vols tv ON cm.ClientSegmentationGroup=tv.ClientSegmentationGroup
		  			          AND cm.ClientStream	  = tv.ClientStream
			                  AND cm.ClientStreamId   = tv.ClientStreamId
			                  AND cm.ClientParent	  = tv.ClientParent
			                  AND cm.ClientId		  = tv.ClientId
							  AND cm.SourceSystem	  = tv.SourceSystem
		                   AND cm.KeyModel=tv.KeyModel
			JOIN
		  currmo_tot ct ON cm.ClientSegmentationGroup=ct.ClientSegmentationGroup
		  			          AND cm.ClientStream	  = ct.ClientStream
			                  AND cm.ClientStreamId   = ct.ClientStreamId
			                  AND cm.ClientParent	  = ct.ClientParent
			                  AND cm.ClientId		  = ct.ClientId
							  AND cm.SourceSystem	  = ct.SourceSystem
		                   AND cm.keymodel=ct.KeyModel
						   AND cm.rpt_month=ct.rpt_month
			JOIN
		  yield_tot yt ON cm.ClientSegmentationGroup=yt.ClientSegmentationGroup
		  			          AND cm.ClientStream	  = yt.ClientStream
			                  AND cm.ClientStreamId   = yt.ClientStreamId
			                  AND cm.ClientParent	  = yt.ClientParent
			                  AND cm.ClientId		  = yt.ClientId
							  AND cm.SourceSystem	  = yt.SourceSystem
		                  AND cm.KeyModel=yt.KeyModel


END;
	
GO


