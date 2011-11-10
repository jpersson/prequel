package net.noerd.prequel

import java.sql.Connection

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

import net.noerd.prequel.SQLFormatterImplicits._
import net.noerd.prequel.ResultSetRowImplicits._

class InTransactionSpec extends Spec with ShouldMatchers with BeforeAndAfterEach {
    
    val database = TestDatabase.config
    
    override def beforeEach() = database.transaction { tx =>
        tx.execute( "create table intransactionspec(id int, name varchar(265))" )
    }
    
    override def afterEach() = database.transaction { tx =>
        tx.execute( "drop table intransactionspec" )
    }
    
    describe( "database.transaction" ) {
        
        it( "should commit after block has been executed" ) {
            database.transaction { tx =>
                tx.execute( "insert into intransactionspec values(?, ?)", 123, "test" )
            }
            
            database.transaction { tx =>
                val count = tx.selectLong( "select count(*) from intransactionspec" )
                
                count should be (1)
            }            
        }

        it( "should rollback if asked to do so" ) {
            database.transaction { tx =>
                tx.execute( "insert into intransactionspec values(?, ?)", 123, "test" )
                tx.rollback()
            }
            
            database.transaction { tx =>
                val count = tx.selectLong( "select count(*) from intransactionspec" )
                
                count should be (0)
            }            
        }

        it( "should rollback if an exception is thrown" ) {
            
            intercept[RuntimeException] {
                database.transaction { tx =>
                    tx.execute( "insert into intransactionspec values(?, ?)", 123, "test" )
                    sys.error( "oh no" )
                }
            }
            
            database.transaction { tx =>    
                val count = tx.selectLong( "select count(*) from intransactionspec" )
                
                count should be (0)
            }            
            
        }
        
        it( "should close the connection after execution finished successfully" ) {
            
            var usedConnection: Connection = null
            
            database.transaction { tx =>                
                tx.execute( "insert into intransactionspec values(?, ?)", 123, "test" )
                usedConnection = tx.connection
            }
            
            usedConnection.isClosed should be (true)
        }

        it( "should close the connection after execution failed" ) {
            
            var usedConnection: Connection = null
            
            database.transaction { tx =>
                intercept[RuntimeException] {
                    tx.execute( "insert into intransactionspec values(?, ?)", 123, "test" )
                    usedConnection = tx.connection
                    sys.error( "oh no" )
                }
            }
            usedConnection.isClosed should be (true)
        }

    }
}
