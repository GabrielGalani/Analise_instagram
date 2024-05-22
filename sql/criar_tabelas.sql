
/*
Arquivo silver TbCabecalho.csv carrega essa tabela
*/
create table TbAccount (
	id_account	varchar (100) not null primary key, 
	account_username varchar (100), 
	account_biography varchar(max), 
	profile_picture_url varchar(max),
	account_name varchar(100)
)


/*
A TABELA ABAIXO SERÁ CARREGADAS PELOS ARQUIVOS TBACCOUNTDAYINSIGHTS e TBACCOUNTLIFE
*/
create table DTbAccountInsights(
	id_insight int not null primary key, -- PK NUMERO UNICO
	name varchar(100),
	frequencia varchar (100),
	title varchar (100),
	description varchar (max)
)


/*
TABELA CARREGADA PELOS ARQUIVOS TBACCOUNTLDAY
*/
create table FTbAccountDayInsights (
	id varchar(200) not null primary key, -- concatenção entre codconta, id_insight, ano, mes, dia, 
	id_insight int, -- FK com a dimensaõ AccountInsight,
	id_account varchar(100), -- FK TbAccount,
	value_last_day int,
	last_end_time date,
	value_actual_day int,
	actual_end_time date,
	extract_date date,
	period_extraction varchar(2),
	year varchar(5),
	day varchar(2),
	foreign key (id_insight) references DTbAccountInsights(id_insight),
	foreign key (id_account) references TbAccount(id_account)
) 	


/*
AS TABELAS ABAIXO SERÃO CARREGADAS PELOS ARQUIVOS TBACCOUNTLIFE**
*/
create table FTbAccountLifetimeInsights (
	id varchar (8000) not null primary key, -- concatenção entre codconta, id_insight, ano, mes, dia, 
	id_account varchar (100), -- FK TbAccount,
	id_insight int,  -- FK com a dimensaõ AccountInsight,
	extract_date date,	
	period_extraction varchar (5),	
	year varchar (5),	
	day varchar (2),
	definicao varchar(100), -- campo name	
	value_descricao varchar(100),	
	value float,
	foreign key (id_insight) references DTbAccountInsights(id_insight),
	foreign key (id_account) references TbAccount(id_account)
)

/*
TABELA SERÁ CARREGADA PELO ARQUIVO TBMEDIAS
*/
create table DTbMidias (
		id_midia varchar not null primary key, -- PK Uma mídia não pode ter dois ID,
		id_account varchar,  -- FK tabela de contas,
		media_type varchar,
		media_url varchar,
		caption varchar,
		timestamp date,
		permalink varchar,
		media_product_type varchar,
		thumbnail_url varchar,
		shortcode varchar,
		foreign key (id_account) references TbAccount(id_account)
)



create table FTbMidias (
		id varchar not null primary key, -- PF concatenação id_midia, year, period, day
		id_midia varchar, -- FK tabela DTbMidias
		id_account varchar, -- FK tabela TbAccount
		comments_count int,
		like_count int,
		extract_date date,
		period varchar,
		year varchar,
		day varchar,
		foreign key (id_account) references TbAccount(id_account)
		foreign key (id_midia) references DTbMidias(id_midia)
)


/*
TABELA SERÁ CARREGADA PELOS ARQUIVOS MIDIAS INSIGHTS E STORIES INSIGHTS
*/
create table DTbDescricaoInsights (
	id varchar not null primary key, -- ID DA DESCRIÇÃO
	name varchar,
	frequencia varchar,
	title varchar,
	description varchar
)


create table FTbMidiasInsights (
	id varchar not null primary key, -- PK concatenação entre id_midia, ano, periodo, dia, id_insight 1800527391836994020240522reachlifetime
	id_midia varchar, -- FK tabela de midia
	id_insight varchar,  -- FK tabela de Descricao Insight
	extract_date varchar,
	period_extraction varchar,
	year varchar,
	day varchar,
	value float,
	foreign key (id_account) references TbAccount(id_account)
	foreign key (id_insight) references DTbDescricaoInsights(id_insight)
)