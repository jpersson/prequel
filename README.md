Prequel - SQL is enough
=======================

There are a lot of database libraries out there. Most of them try to create a new abstraction on top of SQL. I think SQL is already a quite nice abstraction for working with data. Prequel aims to make working with this abstraction a bit more comfortable, nothing more.

[![Build Status](https://secure.travis-ci.org/jpersson/prequel.png)](http://travis-ci.org/jpersson/prequel)

### Background

Prequel is a small set of classes making handling of SQL queries in Scala a bit easier. It takes care of connection handling/pooling, sql escaping, parameter conversion and to some extent transaction handling.

Prequel was written by me, [Johan Persson](https://github.com/jpersson) since I was not really happy with what I could find in terms of jdbc based database libraries. The library is heavily inspired by projects like [Querulous](https://github.com/nkallen/querulous), [Simplifying JDBC](http://scala.sygneca.com/code/simplifying-jdbc) and unreleased work of [Tristan Juricek](https://github.com/tristanjuricek).

See example below how prequel can make your life easier.

### Database Compatibility

Prequel should be compatible with most JDBC supported databases. I've only tested it using HSQLDB and PostgreSQL but MySQL and others should work fine. 

### Use at your own risk

Although I'm using this library in my own projects I have not tested it with massive amounts of data so use at your own risk :-)

### Not supported

 * Logging (will be implemented later on)
 * Any config files for database configuration
 * Any type of ORM voodoo (and will never be)

Examples
--------

Given the following import and definitions

```scala
import net.noerd.prequel.DatabaseConfig
import net.noerd.prequel.SQLFormatterImplicits._
import net.noerd.prequel.ResultSetRowImplicits._

case class Bicycle( id: Long, brand: String, releaseDate: DateTime )

val database = DatabaseConfig(
    driver = "org.hsqldb.jdbc.JDBCDriver",
    jdbcURL = "jdbc:hsqldb:mem:mymemdb"
)
```

Prequel makes it quite comfortable for you to do:

## Inserts

```scala
def insertBicycle( bike: Bicycle ): Unit = {
    database.transaction { tx => 
        tx.execute( 
            "insert into bicycles( id, brand, release_date ) values( ?, ?, ? )", 
            bike.id, bike.brand, bike.releaseDate
        )
    }
}
```
## Batch Updates and Inserts

```scala
def insertBicycles( bikes: Seq[ Bicycle ] ): Unit = {
    database.transaction { tx => 
      tx.executeBatch( "insert into bicycles( id, brand, release_date ) values( ?, ?, ? )" ) { statement => 
        bikes.foreach { bike =>
          statment.executeWith( bike.id, bike.brand, bike.releaseDate )
        }
      }
    }
}
```
 
## Easily create objects from selects

```scala
def fetchBicycles(): Seq[ Bicycles ] = {
    database.transaction { tx => 
        tx.select( "select id, brand, release_date from bicycles" ) { r =>
            Bicycle( r, r, r )
        }
    }
}
```

## Select native types directly

```scala
def fetchBicycleCount: Long = {
    database.transaction { tx => 
        tx.selectLong( "select count(*) from bicycles")
    }
}
```

Use in your Project
-------------------

Releases of Prequel are published to [oss.sonatype.org](https://oss.sonatype.org/content/groups/public). If you want to use it in your project just add it as a dependency or download the jar file directly.

### SBT

```
"net.noerd" %% "prequel" % "0.3.9"
```
    
Dependencies
------------

### 3rd Party libs

I've tried to keep the list of dependencies as short as possible but currently the following
libraries are being used.

* [commons-pool 1.5.5](http://commons.apache.org/pool) for general object pooling
* [commons-dbcp 1.4](http://commons.apache.org/dbcp) for the more db specific parts of connection pools
* [commons-lang 2.6](http://commons.apache.org/lang) for SQL escaping
* [joda-time 1.6.2](http://joda-time.sourceforge.net/) for sane support of Date and Time

### Testing

For testing I use [scala-test](http://www.scalatest.org) for unit-tests and [hsqldb](http://hsqldb.org) for in process db interaction during tests.

Feedback
--------

If you have any questions or feedback just send me a message here or on [twitter](http://twitter.com/suraken) and if you want to contribute just send a pull request.

License
-------

Prequel is licensed under the [wtfpl](http://sam.zoy.org/wtfpl/).