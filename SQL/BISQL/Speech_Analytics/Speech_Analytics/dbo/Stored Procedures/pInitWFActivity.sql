-- =============================================
-- Author:		Vladislav Pilipets
-- Create date: 8/17/22
-- Description:	Initiate Workflow activity 
-- =============================================
CREATE PROCEDURE [dbo].[pInitWFActivity] 
@session_id		as uniqueidentifier, 
@activity_name	as varchar(50) = null
AS
BEGIN
	SET NOCOUNT ON;

	IF EXISTS (SELECT * 
				FROM Workflow_Activity 
				WHERE session_id = @session_id AND IsProcessed = 0) 
	BEGIN 
		SELECT a.session_id session_id, a.activity_id activity_id, a.activity_name activity_name, 0 IsProcessed, aa.sort, aa.activity_name next_activity_name 
		FROM Workflow_Activity a  
			JOIN Workflow_Activities aa on aa.activity_name = a.activity_name and aa.IsActive = 1 
		WHERE session_id = @session_id 
			AND IsProcessed = 0
		
		RETURN;
	END 

	IF @activity_name IS NULL 
	BEGIN 
		-- get last processed activity 
		SELECT @activity_name = a.activity_name 
		FROM (
			SELECT max(activity_id) activity_id  
			FROM Workflow_Activity 
			WHERE session_id = @session_id) t 
		JOIN Workflow_Activity a on t.activity_id = a.activity_id 
	END

	-- check if activities exist for current session 
	-- * if not exists - insert new activity default activity #1 
	-- * return #1 activity 
	IF NOT EXISTS (SELECT * 
				FROM Workflow_state s 
					JOIN Workflow_Activity a ON a.session_id = s.session_id
				WHERE s.session_id = @session_id)
	BEGIN 
		INSERT INTO Workflow_Activity 
		(activity_name, session_id)
		SELECT activity_name, @session_id  
		FROM Workflow_Activities a 
		WHERE a.sort = 1 
			AND a.IsActive = 1 

		SELECT a.session_id session_id, a.activity_id activity_id, a.activity_name activity_name, 0 IsProcessed, aa.sort, aa.activity_name next_activity_name 
		FROM Workflow_Activity a  
			JOIN Workflow_Activities aa on aa.activity_name = a.activity_name and aa.IsActive = 1  
		WHERE a.activity_id = @@identity 
	END 
	ELSE 
	BEGIN 
		-- if activities for a session exist 
		-- check if current activity exist 
		-- if not exists then insert new activity 
		IF NOT EXISTS (SELECT * 
					FROM Workflow_state s 
						JOIN Workflow_Activity a ON a.session_id = s.session_id
					WHERE s.session_id = @session_id AND a.activity_name = @activity_name) 
		BEGIN 
			INSERT INTO Workflow_Activity 
			(activity_name, session_id)
			VALUES
			(@activity_name, @session_id) 
				
			SELECT @session_id session_id, @@identity activity_id, @activity_name activity_name, 0 IsProcessed, a.sort, a.activity_name next_activity_name 
			FROM Workflow_Activities a 
			WHERE a.activity_name = @activity_name and a.IsActive = 1  
		END
		ELSE 
		BEGIN 
			-- ** if not processed - return current activity 
			-- ** if processed - insert next activity
			-- ** ** return next activity 
			IF EXISTS (SELECT * 
					FROM Workflow_state s 
						JOIN Workflow_Activity a ON a.session_id = s.session_id
					WHERE s.session_id = @session_id 
						AND a.activity_name = @activity_name 
						AND a.IsProcessed = 0) 
			BEGIN 
				SELECT s.session_id, a.activity_id, a.activity_name, a.IsProcessed, aa.sort, aa.activity_name next_activity_name  
					FROM Workflow_state s 
						JOIN Workflow_Activity a ON a.session_id = s.session_id 
						JOIN Workflow_Activities aa on aa.activity_name = a.activity_name and aa.IsActive = 1  
					WHERE s.session_id = @session_id 
						AND a.activity_name = @activity_name 
						AND a.IsProcessed = 0
			END 
			ELSE 
			BEGIN 
				INSERT INTO Workflow_Activity 
				(session_id, activity_name)
				SELECT session_id, next_activity_name  
				FROM (
				SELECT s.session_id, a.activity_id, a.activity_name, a.IsProcessed, aa1.sort, aa1.activity_name next_activity_name  
					FROM Workflow_state s 
						JOIN Workflow_Activity a ON a.session_id = s.session_id 
						JOIN Workflow_Activities aa on aa.activity_name = a.activity_name and aa.IsActive = 1  
						JOIN Workflow_Activities aa1 on aa1.sort = (aa.sort + 1)  and aa1.IsActive = 1 
					WHERE s.session_id = @session_id 
						AND a.activity_name = @activity_name 
						AND a.IsProcessed = 1) t 

				SELECT a.session_id session_id, a.activity_id activity_id, a.activity_name activity_name, 0 IsProcessed, aa.sort, aa.activity_name next_activity_name 
				FROM Workflow_Activity a  
					JOIN Workflow_Activities aa on aa.activity_name = a.activity_name and aa.IsActive = 1  
				WHERE a.activity_id = @@identity 
			END 
		END
	END 
END
GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWFActivity] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWFActivity] TO [CORP\pjain]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWFActivity] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWFActivity] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWFActivity] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT VIEW DEFINITION
    ON OBJECT::[dbo].[pInitWFActivity] TO [CORP\aramugade]
    AS [dbo];

