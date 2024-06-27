/*
Arquivo silver TbCabecalho.csv carrega essa tabela
*/

USE DW
if not exists (select * from sysobjects where name='TbAccount' and xtype='U')
create table TbAccount (
	id_account	varchar (100) not null primary key, 
	account_username varchar (100), 
	account_biography varchar(max), 
	profile_picture_url varchar(max),
	account_name varchar(100),
	followers_count int, 
	follows_count int, 
	media_count int,
	definicao varchar(25)
)
/*
A TABELA ABAIXO SERÁ CARREGADAS PELOS ARQUIVOS TBACCOUNTDAYINSIGHTS e TBACCOUNTLIFE
*/
USE DW
if not exists (select * from sysobjects where name='DTbDescricaoInsights' and xtype='U')
create table DTbDescricaoInsights(
	id_insight varchar(100) not null primary key, -- PK NUMERO UNICO
	name varchar(100),
	frequencia varchar (100),
	title varchar (100),
	description varchar (max)
)


USE DW
if not exists (select * from sysobjects where name='FTbAccount' and xtype='U')
create table FTbAccount (
	id varchar(150) not null primary key,
	id_account	varchar (100),
	account_username varchar (100), 
	account_biography varchar(max), 
	profile_picture_url varchar(max),
	account_name varchar(100),
	followers_count int, 
	follows_count int, 
	media_count int,
	extract_date date,
	period varchar(5),
	year varchar(5),
	day varchar(5)
	foreign key (id_account) references TbAccount(id_account)
)

/*
TABELA CARREGADA PELOS ARQUIVOS TBACCOUNTLDAY
*/
USE DW
if not exists (select * from sysobjects where name='FTbAccountDayInsights' and xtype='U')
create table FTbAccountDayInsights (
	id varchar(200) not null primary key, -- concatenção entre codconta, id_insight, ano, mes, dia, 
	id_insight varchar(100), -- FK com a dimensaõ AccountInsight,
	id_account varchar(100), -- FK TbAccount,
	value_last_day int,
	last_end_time date,
	value_actual_day int,
	actual_end_time date,
	extract_date date,
	period_extraction varchar(2),
	year varchar(5),
	day varchar(2),
	foreign key (id_insight) references DTbDescricaoInsights(id_insight),
	foreign key (id_account) references TbAccount(id_account)
) 	


/*
AS TABELAS ABAIXO SERÃO CARREGADAS PELOS ARQUIVOS TBACCOUNTLIFE**
*/
USE DW
if not exists (select * from sysobjects where name='FTbAccountLifetimeInsights' and xtype='U')
create table FTbAccountLifetimeInsights (
	id varchar (8000) not null primary key, -- concatenção entre codconta, id_insight, ano, mes, dia, 
	id_account varchar (100), -- FK TbAccount,
	id_insight varchar(100),  -- FK com a dimensaõ AccountInsight,
	extract_date date,	
	period_extraction varchar (5),	
	year varchar (5),	
	day varchar (2),
	definicao varchar(100), -- campo name	
	value_descricao varchar(100),	
	value float,
	foreign key (id_insight) references DTbDescricaoInsights(id_insight),
	foreign key (id_account) references TbAccount(id_account)
)

/*
TABELA SERÁ CARREGADA PELO ARQUIVO TBMEDIAS
*/
USE DW
if not exists (select * from sysobjects where name='DTbMidias' and xtype='U')
create table DTbMidias (
		id_midia varchar(100) not null primary key, -- PK Uma mídia não pode ter dois ID,
		id_account varchar (100),  -- FK tabela de contas,
		media_type varchar (25),
		media_url varchar (1000),
		caption varchar(max),
		timestamp DATETIMEOFFSET,
		-- timestamp date,
		permalink varchar(max),
		media_product_type varchar(25),
		thumbnail_url varchar (max),
		shortcode varchar (50),
		foreign key (id_account) references TbAccount(id_account)
)


USE DW
if not exists (select * from sysobjects where name='DTbcarrossel' and xtype='U')
create table DTbCarrossel (
	id	varchar (100) not null primary key, 
	media_type varchar (50), 
	type varchar(50), 
	media_url varchar(max),
	id_midia varchar(100)
	foreign key (id_midia) references DTbMidias(id_midia)
)

USE DW
if not exists (select * from sysobjects where name='FTbMidias' and xtype='U')
create table FTbMidias (
		id varchar (100) not null primary key, -- PF concatenação id_midia, year, period, day
		id_midia varchar (100), -- FK tabela DTbMidias
		id_account varchar (100), -- FK tabela TbAccount
		comments_count int,
		like_count int,
		extract_date date,
		period varchar (3),
		year varchar (5),
		day varchar (3),
		foreign key (id_account) references TbAccount(id_account),
		foreign key (id_midia) references DTbMidias(id_midia)
)

/*
TABELA SERÁ CARREGADA PELOS ARQUIVOS MIDIAS INSIGHTS E STORIES INSIGHTS
*/

USE DW
if not exists (select * from sysobjects where name='FTbMidiasInsights' and xtype='U')
create table FTbMidiasInsights (
	id varchar (100) not null primary key, -- PK concatenação entre id_midia, ano, periodo, dia, id_insight 1800527391836994020240522reachlifetime
	id_midia varchar (100), -- FK tabela de midia
	id_insight varchar (100),  -- FK tabela de Descricao Insight
	extract_date date,
	period_extraction varchar (3),
	year varchar (3),
	day varchar (3),
	definicao varchar(100), -- usar o name
	value int,
	foreign key (id_midia) references DTbMidias(id_midia),
	foreign key (id_insight) references DTbDescricaoInsights(id_insight)
)