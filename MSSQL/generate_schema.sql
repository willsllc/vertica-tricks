-- this script queries the INFORMATION_SCHEMA of MSSQL and generates a CSV file that can be used with generate_schema.py

create table #schema 
(
	[schema] varchar(500),
	[table] varchar(500),
	[column] varchar(500),
	[data_type] varchar(500),
	[precision] int null,
	[is_null] int,
	[is_pk] int
)
;

create table #dtmap
(
	[mssql] varchar(255),
	[vertica] varchar(255) null
)
;

INSERT INTO #dtmap ([mssql],[vertica]) values ('varchar','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('uniqueidentifier','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('tinyint','int');
INSERT INTO #dtmap ([mssql],[vertica]) values ('text','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('smallint','int');
INSERT INTO #dtmap ([mssql],[vertica]) values ('smalldatetime','datetime');
INSERT INTO #dtmap ([mssql],[vertica]) values ('nvarchar','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('ntext','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('nchar','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('int','int');
INSERT INTO #dtmap ([mssql],[vertica]) values ('image',NULL);
INSERT INTO #dtmap ([mssql],[vertica]) values ('geometry',NULL);
INSERT INTO #dtmap ([mssql],[vertica]) values ('geography',NULL);
INSERT INTO #dtmap ([mssql],[vertica]) values ('float','float');
INSERT INTO #dtmap ([mssql],[vertica]) values ('decimal','float');
INSERT INTO #dtmap ([mssql],[vertica]) values ('datetime','datetime');
INSERT INTO #dtmap ([mssql],[vertica]) values ('char','varchar');
INSERT INTO #dtmap ([mssql],[vertica]) values ('bit','int');
INSERT INTO #dtmap ([mssql],[vertica]) values ('bigint','int');



insert into #schema ([schema],[table],[column],[data_type],[precision],[is_null],[is_pk])
select
	c.TABLE_SCHEMA as [schema],
	c.TABLE_NAME as [table],
	c.COLUMN_NAME as [column],
	c.DATA_TYPE as [data_type],
	COALESCE(
		c.CHARACTER_MAXIMUM_LENGTH,
		c.NUMERIC_PRECISION,
		c.DATETIME_PRECISION
	) as [precision],
	case when c.IS_NULLABLE='YES' then 1 else 0 end as [is_null],
	case when isnull(pk.ORDINAL_POSITION,0)>0 then 1 else 0 end as [is_pk]
from INFORMATION_SCHEMA.COLUMNS c
left join (
	select TABLE_NAME,TABLE_SCHEMA,COLUMN_NAME,ORDINAL_POSITION
	from INFORMATION_SCHEMA.KEY_COLUMN_USAGE
	where OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
) pk on c.TABLE_SCHEMA=pk.TABLE_SCHEMA and c.TABLE_NAME=pk.TABLE_NAME and c.COLUMN_NAME=pk.COLUMN_NAME
order by 
	c.TABLE_NAME,
	c.ORDINAL_POSITION
;


select
	s.[schema],
	s.[table],
	s.[column],
	s.is_null,
	s.is_pk,
	m.mssql as [datatype_mssql],
	m.vertica as [datatype_vertica],
	s.precision
from #schema s
join #dtmap m on s.data_type=m.mssql



drop table #schema;
drop table #dtmap;
