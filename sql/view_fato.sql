USE [DW]
GO

/****** Object:  View [dbo].[vFactMidias]    Script Date: 29/05/2024 11:16:12 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





ALTER view [dbo].[vFactMidias] as 
select 
	ac.id_account,
	ac.account_username,
	ac.definicao,
	dm.id_midia,
	dm.media_type,
	case when dm.media_url is null then dm.thumbnail_url else dm.media_url end as media_url,
	dm.timestamp,
	dm.media_product_type,
	fm.extract_date,
	sum(fm.comments_count) as comments_count,
	sum(fm.like_count) as like_count,
	case when sum(engagement) is null then round(((cast(sum(comments_count) as  float) + cast(sum(like_count) as float)) / sum(ac.followers_count)) *100,3)
		else 
			round(cast(sum(engagement) as float) / sum(ac.followers_count)*100,3)
		end 
	as engagement,
	sum(exits) as exits,
	sum(impressions) as impressions,
	sum(reach) as reach,
	sum(replies) as replies,
	sum(saved) as saved,
	sum(taps_back) as taps_back,
	sum(taps_forward) as taps_forward,
	datename(weekday, cast(timestamp as datetimeoffset) at time zone 'utc') as dia_semana,
	cast(year(timestamp) as varchar) + cast(datepart(week, timestamp) as varchar) as semana_ano,
	count(dm.id_midia) over (partition by ac.id_account, fm.extract_date, cast(year(timestamp) as varchar) + cast(datepart(week, timestamp) as varchar)) as qtd_publi_na_semana,
	count(dm.id_midia) over (partition by ac.id_account, fm.extract_date, cast(year(timestamp) as varchar) + cast(datepart(week, timestamp) as varchar), datename(weekday, cast(timestamp as datetimeoffset) at time zone 'utc')) as qtd_publi_no_dia_semana,
	'clipping_date=ano'+ cast(year(fm.extract_date) as varchar)+ ' mes'+cast(month(fm.extract_date) as varchar) + ' dia'+cast(day(fm.extract_date) as varchar) as clipping_date
from tbaccount ac
	inner join DTbMidias dm on (dm.id_account = ac.id_account)
	inner join FTbMidias fm on (fm.id_midia = dm.id_midia and fm.id_account = ac.id_account)
	left join (
				select 
					id_midia,
					max(case when definicao = 'engagement' then value end) as engagement,
					max(case when definicao = 'exits' then value end) as exits,
					max(case when definicao = 'impressions' then value end) as impressions,
					max(case when definicao = 'reach' then value end) as reach,
					max(case when definicao = 'replies' then value end) as replies,
					max(case when definicao = 'saved' then value end) as saved,
					max(case when definicao = 'taps_back' then value end) as taps_back,
					max(case when definicao = 'taps_forward' then value end) as taps_forward
				from FTbMidiasInsights
				group by 
					id_midia
	) fmi on (fmi.id_midia = dm.id_midia)

group by 
	ac.id_account,
	ac.account_username,
	ac.definicao,
	dm.id_midia,
	dm.media_type,
	dm.media_url,
	dm.timestamp,
	dm.media_product_type,
	ac.followers_count,
	fm.extract_date,
	dm.thumbnail_url
GO


