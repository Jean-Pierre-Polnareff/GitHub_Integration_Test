CREATE TABLE [dbo].[CM_DSCREP] (
    [EOM]                          DATE           NULL,
    [AGNTID]                       NVARCHAR (100) NULL,
    [CALLID]                       FLOAT (53)     NULL,
    [CALLDT]                       DATE           NULL,
    [RECDP]                        NVARCHAR (15)  NULL,
    [RGSACC]                       NVARCHAR (100) NULL,
    [DIS1]                         NVARCHAR (100) NULL,
    [CLDUR]                        FLOAT (53)     NULL,
    [PHNUMB]                       NVARCHAR (15)  NULL,
    [RGSSEID]                      NVARCHAR (100) NULL,
    [CXLANG]                       NVARCHAR (255) NULL,
    [CXTME]                        FLOAT (53)     NULL,
    [AMLANG]                       NVARCHAR (255) NULL,
    [AMTME]                        FLOAT (53)     NULL,
    [CXFLG]                        INT            NOT NULL,
    [AMFLG]                        INT            NOT NULL,
    [CALLCNT]                      INT            NOT NULL,
    [ACC_Status]                   VARCHAR (5)    NULL,
    [Actual_RPCDetail]             VARCHAR (50)   NULL,
    [DCName_Actual]                VARCHAR (50)   NULL,
    [restriction_Home]             SMALLINT       NULL,
    [restriction_job]              SMALLINT       NULL,
    [restriction_Calls]            SMALLINT       NULL,
    [restriction_suppressletters]  SMALLINT       NULL,
    [restriction_lettertoattorney] SMALLINT       NULL,
    [restriction_commnets]         NVARCHAR (MAX) NULL,
    [attrny_Name]                  VARCHAR (50)   NULL,
    [attrny_Firm]                  VARCHAR (100)  NULL,
    [attrny_Address]               VARCHAR (50)   NULL,
    [attrny_Phone]                 VARCHAR (20)   NULL,
    [attorney_comments]            NVARCHAR (MAX) NULL,
    [Case_Number]                  VARCHAR (50)   NULL,
    [Datefiled]                    DATE           NULL,
    [Legal_Status_Description]     NVARCHAR (MAX) NULL,
    [cccs_Company_Name]            VARCHAR (100)  NULL,
    [cccs_Contact]                 VARCHAR (100)  NULL,
    [cccs_Address]                 VARCHAR (50)   NULL,
    [cccs_Phone]                   VARCHAR (50)   NULL,
    [cccs_Fax]                     VARCHAR (50)   NULL
);




GO
CREATE NONCLUSTERED INDEX [IX_CM_DSCREP_CALLID]
    ON [dbo].[CM_DSCREP]([CALLID] ASC) WITH (DATA_COMPRESSION = PAGE);


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT UPDATE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT SELECT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT INSERT
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\tkumar]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\pjain]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\musalunke]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\mhuang]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\dmukherji]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\aughodake]
    AS [dbo];


GO
GRANT DELETE
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\aramugade]
    AS [dbo];


GO
GRANT ALTER
    ON OBJECT::[dbo].[CM_DSCREP] TO [CORP\pjain]
    AS [dbo];

