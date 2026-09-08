
--CREATE TABLE [dbo].[RPT_client_envision_disputes_monthly_ClientID](
--	[MONTH_DATE] [DATE] NOT NULL,
--	[Client_ID] [VARCHAR](10) NOT NULL,
--	[Envision_disputes] [INT] NOT NULL,
--PRIMARY KEY CLUSTERED 
--(
--	[MONTH_DATE] ASC,[Client_ID] ASC
--)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [DATA]
--) ON [DATA]
--GO
--ALTER TABLE [dbo].[RPT_client_envision_disputes_monthly_ClientID]  WITH CHECK ADD CHECK  (([Envision_disputes]>=(0)))
--GO

CREATE PROC [dbo].[rpt_pull_client_envision_disputes_monthly_ClientID]
AS

DECLARE 
@dMonth	DATE = ISNULL((SELECT MAX(MONTH_DATE) FROM RPT_client_envision_disputes_monthly_ClientID),'2020-11-01'),
@dStart	DATE = DATEADD(MONTH,-1,DATEADD(day,-DATEPART(day,GETDATE())+1,GETDATE())),
@dEnd	DATE = DATEADD(DAY,-DATEPART(day,GETDATE())+1,CAST(GETDATE() AS DATE));

IF (@dMonth < @dStart) BEGIN
  INSERT RPT_client_envision_disputes_monthly_ClientID 
  SELECT M.month_date,ISNULL(D.ClientId,''),ISNULL(D.Envision_disputes,0) Envision_disputes FROM 
    (SELECT DATEADD(MONTH,number,@dMonth) month_date FROM [master]..spt_values WHERE [Type] = 'P' AND Number < DATEDIFF(MONTH,@dMonth,@dEnd) AND DATEADD(MONTH,number,@dMonth) > @dMonth) M
     LEFT JOIN 
    (SELECT DATEADD(DAY,-DATEPART(day,dcu.UpdateDate)+1,CAST(dcu.UpdateDate AS DATE)) AS month_date,dcu.ClientId
        , COUNT(*) AS Envision_disputes
     FROM DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK)
          JOIN
        DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON dcu.ClientId=dcl.ClientId
     WHERE dcl.ClientStreamId LIKE 'IMRTI%' 
	 --WHERE dcl.ClientParent LIKE 'PRT%'
         AND dcu.StatusCode='DISPUTE'
         AND dcu.UpdateDate >= @dMonth AND dcu.UpdateDate < @dEnd
     GROUP BY DATEADD(DAY,-DATEPART(day,dcu.UpdateDate)+1,CAST(dcu.UpdateDate AS DATE)),dcu.ClientId
    ) D ON M.month_date = D.month_date ORDER BY M.month_date ASC;
END
ELSE IF (@dMonth = @dStart) BEGIN
  UPDATE M SET M.Envision_disputes = ED.Envision_disputes FROM RPT_client_envision_disputes_monthly_ClientID M JOIN (
    SELECT @dMonth AS month_date,dcu.ClientId
         , COUNT(*) AS Envision_disputes
    FROM DW_MSTR_DM.dbo.DimCustomer dcu (NOLOCK)
         JOIN
     DW_MSTR_DM.dbo.DimClient dcl (NOLOCK) ON dcu.ClientId=dcl.ClientId
  WHERE dcl.ClientStreamId LIKE 'IMRTI%' 
  --WHERE dcl.ClientParent LIKE 'PRT%'
      AND dcu.StatusCode='DISPUTE'
         AND dcu.UpdateDate >= @dMonth AND dcu.UpdateDate < DATEADD(MONTH,1,@dMonth)
     GROUP BY dcu.ClientId
  ) ED ON M.MONTH_DATE = ED.MONTH_DATE AND M.Client_ID = ED.ClientId;
END;
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT EXECUTE
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [corp\ravijaykumar]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[rpt_pull_client_envision_disputes_monthly_ClientID] TO [CORP\aramugade]
    AS [dbo];

