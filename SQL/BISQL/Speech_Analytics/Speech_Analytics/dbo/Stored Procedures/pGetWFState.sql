-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 8/17/22
-- Description:	Get current workflow state 
-- =============================================
CREATE PROCEDURE [dbo].[pGetWFState]

AS
BEGIN
	SET NOCOUNT ON;

	DELETE 
	FROM Workflow_Activity 
	WHERE activity_name = '' 
		AND IsProcessed = 0  

    DROP TABLE IF EXISTS #temp_wf_state

	SELECT s.session_id, last_started activity_id, a.activity_name, a.IsProcessed, aa.sort, a.activity_name next_activity_name, update_time  
	INTO #temp_wf_state
	FROM Workflow_State s 
		LEFT JOIN Workflow_Activity a ON a.session_id = s.session_id 
		LEFT JOIN Workflow_Activities aa on aa.activity_name = a.activity_name and aa.IsActive = 1 
	WHERE s.IsProcessed = 0 
		and a.IsProcessed = 0 
	ORDER BY update_time DESC  
	 
	IF (SELECT COUNT(1) FROM #temp_wf_state) > 0 
	BEGIN 
		SELECT session_id, activity_id, activity_name, IsProcessed, sort, next_activity_name FROM #temp_wf_state ORDER BY update_time 
	END 
	ELSE 
	BEGIN 
		SELECT t.session_id, t.activity_id, a.activity_name, a.IsProcessed, aa.sort, aa1.activity_name next_activity_name  
		FROM (
				SELECT MAX(s.session_id) session_id, MAX(a.activity_id) activity_id  
				FROM Workflow_State s 
					JOIN Workflow_Activity a ON a.session_id = s.session_id 
				WHERE s.IsProcessed = 0 ) t 
			join Workflow_Activity a on a.activity_id = t.activity_id 
			join Workflow_Activities aa on a.activity_name = aa.activity_name and aa.IsActive = 1 
			join Workflow_Activities aa1 on aa1.sort = (aa.sort + 1) and aa1.IsActive = 1 
	END  

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetWFState] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetWFState] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetWFState] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetWFState] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetWFState] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pGetWFState] TO [CORP\aramugade]
    AS [dbo];

