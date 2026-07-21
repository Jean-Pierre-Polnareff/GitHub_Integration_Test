CREATE proc [dbo].[usp_Radius_call_Stats]
as
begin
declare @StartDate date = GETDATE() - 7
IF OBJECT_ID('tempdb..#tDaily') IS NOT NULL DROP TABLE #tDaily;

		SELECT [Call_Date],CGWQNAME [Service_Name],CGDIALID Service_Type,CASE WHEN CALL_TYPE = 'IN' THEN 'Inbound' ELSE 'Outbound' END Transaction_Type
		INTO #tDaily FROM DW_MSTR_DM.dbo.Call_History_Fact (NOLOCK) WHERE call_date >=@startdate

		DELETE CLIENT_ANALYTICS.dbo.RadiusCallStats WHERE Call_Date IN (SELECT DISTINCT Call_Date FROM #tDaily) AND LV_Client_Name = 'Northland_Group';

		INSERT CLIENT_ANALYTICS.dbo.RadiusCallStats(Call_Date,Call_Center_Name,Call_Center_Id,LV_Client_Name,[Service_Name],Service_Type,Service_Id,Transaction_Type,Data_Source_File_Name,Total_Record_Count)
		SELECT Call_Date,
			'' Call_Center_Name,
			NULL Call_Center_Id,
			'Northland_Group' LV_Client_Name,
			[Service_Name],
			Service_Type,
			NULL Service_Id,
			Transaction_Type,
			'' Data_Source_File_Name,
			COUNT(*) Total_Record_Count 
		FROM #tDaily GROUP BY Call_Date,[Service_Name],Service_Type,Transaction_Type;	

		IF OBJECT_ID('tempdb..#tDaily1') IS NOT NULL DROP TABLE #tDaily1;

		SELECT Call_Date,Call_Center_Name,Call_Center_Id,LV_Client_Name,[Service_Name],Service_Type,Service_Id,Transaction_Type,Data_Source_File_Name
		INTO #tDaily1 FROM DW_MSTR_DM.dbo.RadiusCall (NOLOCK) WHERE Call_Date  >=@startdate

		DELETE CLIENT_ANALYTICS.dbo.RadiusCallStats WHERE Call_Date IN (SELECT DISTINCT Call_Date FROM #tDaily1) AND LV_Client_Name <> 'Northland_Group';

		INSERT CLIENT_ANALYTICS.dbo.RadiusCallStats(Call_Date,Call_Center_Name,Call_Center_Id,LV_Client_Name,[Service_Name],Service_Type,Service_Id,Transaction_Type,Data_Source_File_Name,Total_Record_Count)
		SELECT Call_Date,Call_Center_Name,Call_Center_Id,LV_Client_Name,[Service_Name],Service_Type,Service_Id,Transaction_Type,Data_Source_File_Name,COUNT(*) Total_Record_Count 
		FROM #tDaily1 GROUP BY Call_Date,Call_Center_Name,Call_Center_Id,LV_Client_Name,[Service_Name],Service_Type,Service_Id,Transaction_Type,Data_Source_File_Name;
		end