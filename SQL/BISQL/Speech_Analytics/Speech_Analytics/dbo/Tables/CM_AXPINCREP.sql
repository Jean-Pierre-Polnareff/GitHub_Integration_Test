CREATE TABLE [dbo].[CM_AXPINCREP] (
    [EOM]         DATE           NULL,
    [AGNTID]      NVARCHAR (100) NULL,
    [CALLID]      BIGINT         NULL,
    [CALLDT]      DATE           NULL,
    [RECDP]       NVARCHAR (15)  NULL,
    [RGSACC]      NVARCHAR (100) NULL,
    [DIS1]        NVARCHAR (100) NULL,
    [SKNM]        NVARCHAR (100) NULL,
    [CLDUR]       FLOAT (53)     NULL,
    [DIR]         NVARCHAR (50)  NULL,
    [RGSSEID]     NVARCHAR (100) NULL,
    [PHNUMB]      NVARCHAR (15)  NULL,
    [CLNTID]      NVARCHAR (100) NULL,
    [SOLTIME]     FLOAT (53)     NULL,
    [SUCTIME]     FLOAT (53)     NULL,
    [CLLCNT]      INT            NULL,
    [CXSUCC]      INT            NULL,
    [REPTYPE]     NVARCHAR (20)  NULL,
    [File_Number] INT            NULL,
    [received]    DATE           NULL,
    [closed]      DATE           NULL,
    [returned]    DATE           NULL,
    [ACCSTATUS]   VARCHAR (20)   NULL,
    [ACCBAL]      MONEY          NULL,
    [COL1]        NVARCHAR (50)  NULL,
    [COL2]        NVARCHAR (50)  NULL,
    [COL3]        NVARCHAR (50)  NULL,
    [COL4]        NVARCHAR (50)  NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_CM_AXPINCREP_CALLID]
    ON [dbo].[CM_AXPINCREP]([CALLID] ASC);




GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_AXPINCREP] TO [CORP\aramugade]
    AS [dbo];

