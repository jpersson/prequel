package net.noerd.prequel

import java.util.Date
import java.sql.SQLException

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter
import net.noerd.prequel.ResultSetRowImplicits._

class ResultSetRowSpec extends Spec with ShouldMatchers with BeforeAndAfterEach {
        
    implicit val databaseConfig = TestDatabase.config
    
    describe( "ResultSetRow" ) {
        
        it( "should return the column names of the row" ) {
            InTransaction { tx =>
                val expectedColumns = Seq( "id", "name" )
                tx.execute( "create table resultsetrowspec(id int, name varchar(265))" )
                tx.execute( "insert into resultsetrowspec values(%s, %s)", 242, "test1" )                
                tx.select("select id, name from resultsetrowspec") { row =>
                    row.columnNames should equal (expectedColumns)
                    row.nextString
                }
            }
        }

        it( "should return a Boolean" ) { InTransaction { tx =>
            val value = false
            tx.execute( "create table boolean_table(c1 boolean)" )
            tx.execute( "insert into boolean_table values(%s)", value)
            tx.select( "select c1 from boolean_table" ) { row =>
                row.nextBoolean should equal (value)
            }
        } }
        
        it( "should return a Long" ) { InTransaction { tx =>
            val value = 12345
            tx.execute( "create table long_table(c1 int)" )
            tx.execute( "insert into long_table values(%s)", value)
            tx.select( "select c1 from long_table" ) { row =>
                row.nextLong should equal (value)
            }
        } }
        
        it( "should return an Int" ) { InTransaction { tx =>
            val value = 12345
            tx.execute( "create table int_table(c1 int)" )
            tx.execute( "insert into int_table values(%s)", value)
            tx.select( "select c1 from int_table" ) { row =>
                row.nextInt should equal (value)
            }
        } }
        
        it( "should return a String" ) { InTransaction { tx =>
            val value = "test"
            tx.execute( "create table string_table(c1 varchar(256))" )
            tx.execute( "insert into string_table values(%s)", value)
            tx.select( "select c1 from string_table" ) { row =>
                row.nextString should equal (value)
            }
        } }

        it( "should return a DateTime" ) { InTransaction { tx =>
            val value = new DateTime
            tx.execute( "create table datetime_table(c1 timestamp)" )
            tx.execute( "insert into datetime_table values(%s)", value )
            tx.select( "select c1 from datetime_table" ) { row =>
                row.nextDateTime should equal (value)
            }
        } }

        it( "should return a Duration" ) { InTransaction { tx =>
            val value = Duration.standardDays( 43 )
            tx.execute( "create table duration_table(c1 bigint)" )
            tx.execute( "insert into duration_table values(%s)", value )
            tx.select( "select c1 from duration_table" ) { row =>
                row.nextDuration should equal (value)
            }
        } }

        it( "should return a Date" ) { InTransaction { tx =>
            val value = new Date
            tx.execute( "create table date_table(c1 timestamp)" )
            tx.execute( "insert into date_table values(%s)", value )
            tx.select( "select c1 from date_table" ) { row =>
                row.nextDate.getTime should equal (value.getTime)
            }
        } }

        it( "should return a Float" ) { InTransaction { tx =>
            val value = 1.5f
            tx.execute( "create table float_table(c1 real)" )
            tx.execute( "insert into float_table values(%s)", value )
            tx.select( "select c1 from float_table" ) { row =>
                row.nextFloat should equal (value)
            }
        } }


    }
}
