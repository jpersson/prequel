package net.noerd.prequel 

object TestDatabase {
    
    val config = DatabaseConfig(
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:mymemdb",
        sqlFormatter = SQLFormatter.HSQLDBSQLFormatter
    )
}