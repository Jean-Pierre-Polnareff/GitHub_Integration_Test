-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 8/17/22 
-- Description:	Initialize Workflow 
-- =============================================
CREATE PROCEDURE [dbo].[pInitWorkflow] 
	
AS
BEGIN
	SET NOCOUNT ON;

	IF EXISTS (SELECT * 
				FROM Workflow_State s 
					JOIN Workflow_Activity a on a.session_id = s.session_id 
				WHERE s.IsProcessed = 0 AND a.IsProcessed = 0) 
	BEGIN 
		SELECT s.session_id, a.activity_id, a.activity_name  
				FROM Workflow_State s 
					JOIN Workflow_Activity a on a.session_id = s.session_id 
				WHERE s.IsProcessed = 0 AND a.IsProcessed = 0 
	END 
	ELSE 
	BEGIN 
		IF EXISTS (SELECT * 
				FROM (
					SELECT max(aa.sort) sort  
					FROM Workflow_State s 
						JOIN (
							SELECT min(create_date) create_date 
							FROM Workflow_State s 
							WHERE s.IsProcessed = 0) t on t.create_date = s.create_date 
						JOIN Workflow_Activity a ON a.session_id = s.session_id 
						JOIN Workflow_Activities aa ON aa.activity_name = a.activity_name and aa.IsActive = 1) t  
					JOIN Workflow_Activities aa ON aa.sort = t.sort + 1) 
		BEGIN  
			SELECT t.session_id, aa.id activity_id,aa.activity_name  
			FROM (
				SELECT max(aa.sort) sort, s.session_id, max(aa.id) activity_id    
				FROM Workflow_State s 
					JOIN (
						SELECT min(create_date) create_date 
						FROM Workflow_State s 
						WHERE s.IsProcessed = 0) t on t.create_date = s.create_date 
					JOIN Workflow_Activity a ON a.session_id = s.session_id 
					JOIN Workflow_Activities aa ON aa.activity_name = a.activity_name and aa.IsActive = 1
					GROUP BY s.session_id) t  
				JOIN Workflow_Activities aa ON aa.sort = t.sort + 1 and aa.IsActive = 1 
				
			RETURN; 
		END 
		ELSE 
		BEGIN 
			UPDATE s 
			SET IsProcessed = 1 
			FROM Workflow_State s 
				JOIN (
					SELECT min(create_date) create_date 
					FROM Workflow_State s 
					WHERE s.IsProcessed = 0) t on t.create_date = s.create_date 
				JOIN Workflow_Activity a ON a.session_id = s.session_id 
				JOIN Workflow_Activities aa ON aa.activity_name = a.activity_name and aa.IsActive = 1
		END 

		DECLARE @t table (session_id UNIQUEIDENTIFIER, activity_id BIGINT, activity_name VARCHAR(50))
		INSERT INTO Workflow_State 
		(last_started, last_finished)
		OUTPUT inserted.session_id,0,null INTO @t(session_id,activity_id,activity_name)  
		SELECT 1,1 
		SELECT session_id, activity_id, activity_name 
		FROM @t; 
	END 

END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWorkflow] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWorkflow] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWorkflow] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWorkflow] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWorkflow] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWorkflow] TO [CORP\aramugade]
    AS [dbo];

