CREATE TABLE [dbo].[Workflow_Activity_Skips] (
    [activity_id]   BIGINT           NOT NULL,
    [activity_name] VARCHAR (50)     NULL,
    [session_id]    UNIQUEIDENTIFIER NULL,
    [error_msg]     VARCHAR (2000)   NULL,
    [IsProcessed]   BIT              DEFAULT ((0)) NOT NULL,
    [start_time]    DATETIME         DEFAULT (getdate()) NOT NULL,
    [end_time]      DATETIME         NULL,
    [insertdate]    DATETIME         DEFAULT (getdate()) NOT NULL
);

