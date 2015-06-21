
CREATE TABLE [dbo].[bcpDemoTable](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[idNumber] [int] NOT NULL,
	[realNumber] [float] NOT NULL,
	[fixstr] [char](30) NOT NULL,
	[varStr] [varchar](500) NOT NULL,
	[DateValue] [date] NOT NULL,
	[bitValue] [bit] NOT NULL,
 CONSTRAINT [PK_insertTable_1_1] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO
