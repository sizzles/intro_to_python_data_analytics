import sqlite3

connection = sqlite3.connect('films.db')

print('Create BFI_Raw Table')
table_sql = """
	CREATE TABLE IF NOT EXISTS BFI_Raw (
		Id INTEGER PRIMARY KEY AUTOINCREMENT,
		Film VARCHAR(255),
		Rank INT,
		CountryOfOrigin VARCHAR(63),
		WeekendGross INT,
		Distributor VARCHAR(63),
		PercentWeekChange VARCHAR(63),
		WeeksReleased INT,
		NoCinemas INT,
		SiteAvg INT,
		TotalGross INT,
		ReportName VARCHAR(63),
		ReportDate VARCHAR(31),
		ModifiedTime VARCHAR(31),
		ModifiedBy VARCHAR(31))
		"""
	
c = connection.cursor()
c.execute(table_sql)

index_sqls = [
	"""CREATE INDEX IF NOT EXISTS idxBFI_RawFilm ON BFI_Raw(Film)""",
	"""CREATE INDEX IF NOT EXISTS idxBFI_RawReportDate ON BFI_Raw(ReportDate)""",
	"""CREATE INDEX IF NOT EXISTS idxBFI_RawRank ON BFI_Raw(Rank)""",
]

for s in index_sqls:
	c.execute(s)

########### BFI Validated
print('Create BFI_Validated Table')
table_sql = """
	CREATE TABLE IF NOT EXISTS BFI_Validated (
		Id INTEGER PRIMARY KEY AUTOINCREMENT,
		Film VARCHAR(255),
		Rank INT,
		WeekendGross INT,
		Distributor VARCHAR(63),
		PercentWeekChange REAL,
		WeeksReleased INT,
		NoCinemas INT,
		SiteAvg INT,
		TotalGross INT,
		ReportDate VARCHAR(31),
		ModifiedTime VARCHAR(31),
		ModifiedBy VARCHAR(31),
		UNIQUE(ReportDate, Film))"""
	
c = connection.cursor()
c.execute(table_sql)

index_sqls = [
	"""CREATE INDEX IF NOT EXISTS idxBFI_ValidatedFilm ON BFI_Validated(Film)""",
	"""CREATE INDEX IF NOT EXISTS idxBFI_ValidatedReportDate ON BFI_Validated(ReportDate)""",
	"""CREATE INDEX IF NOT EXISTS idxBFI_ValidatedRank ON BFI_Validated(Rank)""",
]

for s in index_sqls:
	c.execute(s)