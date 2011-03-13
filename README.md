Prequel - SQL in Scala
======================

Prequel is a small set of classes making handling of SQL queries in Scala a bit easier. It takes care of connection handling/pooling, sql escaping, parameter conversion and to some extent transaction handling.

Prequel was written by me, [Johan Persson](https://github.com/jpersson) since I was not really happy with what I could find in terms of jdbc based database libraries. The library is heavily influenced by projects like [Querulous](https://github.com/nkallen/querulous), [Simplifying JDBC](http://scala.sygneca.com/code/simplifying-jdbc) and unreleased work of [Tristan Juricek](https://github.com/tristanjuricek).


# Not supported

 * Logging (will be implemented later on)
 * Any config files for database configuration
 * Any type of ORM voodoo (and will never be)

Examples
-------

    import net.noerd.prequel.InTransaction
    import net.noerd.prequel.DatabaseConfig
    import net.noerd.prequel.ResultSetRow.row2String
    import net.noerd.prequel.ResultSetRow.row2Int

    class PrequelTest {
        // The database config should be created only once
        // since it's used during connection pooling
        implicit val databaseConfig = DatabaseConfig( 
            driver = "org.hsqldb.jdbc.JDBCDriver",
            jdbcURL = "jdbc:hsqldb:mem:mymemdb"
        )
       
        def insertFoo( foo: String ) = {
            InTransaction { tx => 
                tx.execute( "insert into foo(%s)", foo )
            }
        }
        
        def selectFoo() = {
            InTransaction { tx => 
                tx.select( "select bar from" ) { row =>
                    Foo( row )
                }
            }
        }
    }
    
Dependencies
------------

I've tried to keep the list of dependencies as short as possible but currently the following
libraries are being used.

* [commons-pool 1.5.5](http://commons.apache.org/pool) for general object pooling
* [commons-dbcp 1.4](http://commons.apache.org/dbcp) for the more db specific parts of connection pools
* [commons-lang 2.6](http://commons.apache.org/lang) for SQL escaping
* [joda-time 1.6.2](http://joda-time.sourceforge.net/) for sane support of Date and Time

# Testing

For testing I use [scala-test](http://www.scalatest.org) for unit-tests and [hsqldb](http://hsqldb.org) for in process db interaction during tests.

Feedback
--------

If you have any questions or feedback just send me a message here or on [twitter](http://twitter.com/suraken) and if you want to contribute just send a pull request.