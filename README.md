Prequel - SQL in Scala
======================

Prequel is a small set of classes making handling of SQL queries in Scala a bit easier. It takes care of connection handling/pooling, sql escaping, parameter conversion and to some extent transaction handling.

Prequel was written by me, [Johan Persson](https://github.com/jpersson) since I was not really happy with what I could find in terms of jdbc based database libraries. The library is heavily inspired by projects like [Querulous](https://github.com/nkallen/querulous), [Simplifying JDBC](http://scala.sygneca.com/code/simplifying-jdbc) and unreleased work of [Tristan Juricek](https://github.com/tristanjuricek).

See example below how prequel can make your life easier.

## Database Compatibility

Prequel should be compatible with most JDBC supported databases. I've only tested it using HSQLDB and PostgreSQL but MySQL and others should work fine. 

## Use at your own risk

Although I'm using this library in my own projects I have not tested it with massive amounts of data so use at your own risk :-)

## Not supported

 * Logging (will be implemented later on)
 * Any config files for database configuration
 * Any type of ORM voodoo (and will never be)

Example
-------

    ```scala
    import net.noerd.prequel.InTransaction
    import net.noerd.prequel.DatabaseConfig
    import net.noerd.prequel.ResultSetRowImplicits._
    import net.noerd.prequel.SQLFormatterImplicits._    
    
    case class Bicycle( id: Long, brand: String, releaseDate: DateTime)

    object PrequelTest {
        // The database config should be created only once
        // since it's used during connection pooling
        implicit val databaseConfig = DatabaseConfig( 
            driver = "org.hsqldb.jdbc.JDBCDriver",
            jdbcURL = "jdbc:hsqldb:mem:mymemdb"
        )
       
        def insertBicycle( bike: Bicycle ): Unit = {
            InTransaction { tx => 
                tx.execute( 
                    "insert into bicycles( id, brand, release_date ) values( %s, %s, %s )", 
                    bike.id, bike.brand, bike.releaseDate
                )
            }
        }
        
        def fetchBicycles(): Seq[ Bicycles ] = {
            InTransaction { tx => 
                tx.select( "select id, brand, release_date from bicycles" ) { r =>
                    Bicycle( r, r, r )
                }
            }
        }
        
        def fetchBicycleCount: Long = {
            InTransaction { tx => 
                tx.selectLong( "select count(*) from bicycles")
            }
        }
    }
    
Dependencies
------------

## Scala

I'm developing against Scala 2.8.1 but there are not that many 2.8 features used so it should be rather easy to port to 2.7.7 if you still haven't made the leap. 

## 3rd Party libs

I've tried to keep the list of dependencies as short as possible but currently the following
libraries are being used.

* [commons-pool 1.5.5](http://commons.apache.org/pool) for general object pooling
* [commons-dbcp 1.4](http://commons.apache.org/dbcp) for the more db specific parts of connection pools
* [commons-lang 2.6](http://commons.apache.org/lang) for SQL escaping
* [joda-time 1.6.2](http://joda-time.sourceforge.net/) for sane support of Date and Time

## Testing

For testing I use [scala-test](http://www.scalatest.org) for unit-tests and [hsqldb](http://hsqldb.org) for in process db interaction during tests.

Feedback
--------

If you have any questions or feedback just send me a message here or on [twitter](http://twitter.com/suraken) and if you want to contribute just send a pull request.