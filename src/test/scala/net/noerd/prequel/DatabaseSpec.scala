package net.noerd.prequel

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import net.noerd.prequel.ResultSetRow.row2String
import net.noerd.prequel.ResultSetRow.row2Int

class DatabaseSpec extends Spec with ShouldMatchers {
    
    implicit val databaseConfig = DatabaseConfig( 
        driver = "org.hsqldb.jdbc.JDBCDriver",
        jdbcURL = "jdbc:hsqldb:mem:mymemdb"
    )
    
    describe( "Datebase" ) {
        
        it( "should run this test " ) {
            
            case class FooBar( foo: String, bar: Int )
            
            InTransaction { tx => 
                
                tx.execute( "create table foobar(foo varchar(16), bar int)" )
                tx.execute( "insert into foobar values(%s, %s)", "test", 1 )
                
                tx.select( "select foo, bar from foobar" ) { row =>
                    println( ":" + FooBar( row, row ) )
                }
            }

            InTransaction { tx => 
                
                tx.select( "select foo, bar from foobar" ) { row =>
                    println( ":" + FooBar( row, row ) )
                }
            }

        }
    }
}