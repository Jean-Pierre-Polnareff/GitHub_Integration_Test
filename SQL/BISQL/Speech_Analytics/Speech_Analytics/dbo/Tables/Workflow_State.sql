CREATE TABLE [dbo].[Workflow_State] (
    [session_id]    UNIQUEIDENTIFIER DEFAULT (newid()) NOT NULL,
    [last_started]  BIGINT           NULL,
    [last_finished] BIGINT           NULL,
    [IsProcessed]   BIT              DEFAULT ((0)) NOT NULL,
    [create_date]   DATETIME         DEFAULT (getdate()) NOT NULL,
    [update_time]   DATETIME         DEFAULT (getdate()) NOT NULL
);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[Workflow_State] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Workflow_State] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[Workflow_State] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[Workflow_State] TO [CORP\dmukherji]
    AS [dbo];

