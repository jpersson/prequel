package net.noerd.prequel

import java.sql.Connection

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

import net.noerd.prequel.SQLFormatterImplicits._
import net.noerd.prequel.ResultSetRowImplicits._

class InTransactionSpec extends Spec with ShouldMatchers with BeforeAndAfterEach {
    
    implicit val databaseConfig = TestDatabase.config
    
    override def beforeEach() = InTransaction { tx =>
        tx.execute( "create table intransactionspec(id int, name varchar(265))" )
    }
    
    override def afterEach() = InTransaction { tx =>
        tx.execute( "drop table intransactionspec" )
    }
    
    describe( "InTransaction" ) {
        
        it( "should commit after block has been executed" ) {
            
            InTransaction { tx =>
                
                tx.execute( "insert into intransactionspec values(%s, %s)", 123, "test" )
            }
            
            InTransaction { tx =>
                
                val count = tx.selectLong( "select count(*) from intransactionspec" )
                
                count should be (1)
            }            
        }

        it( "should rollback if asked to do so" ) {

            InTransaction { tx =>
                
                tx.execute( "insert into intransactionspec values(%s, %s)", 123, "test" )
                tx.rollback()
            }
            
            InTransaction { tx =>
                
                val count = tx.selectLong( "select count(*) from intransactionspec" )
                
                count should be (0)
            }            
        }

        it( "should rollback if an exception is thrown" ) {

            try {
                InTransaction { tx =>
                
                    tx.execute( "insert into intransactionspec values(%s, %s)", 123, "test" )
                    error( "oh no" )
                }
            }
            catch {
                case e: RuntimeException => // expected
            }
            
            InTransaction { tx =>
                
                val count = tx.selectLong( "select count(*) from intransactionspec" )
                
                count should be (0)
            }            
            
        }
        
        it( "should close the connection after execution finished successfully" ) {
            
            var usedConnection: Connection = null
            
            InTransaction { tx =>
                
                tx.execute( "insert into intransactionspec values(%s, %s)", 123, "test" )
                usedConnection = tx.connection
            }
            
            usedConnection.isClosed should be (true)
        }

        it( "should close the connection after execution failed" ) {
            
            var usedConnection: Connection = null
            
            try {
                InTransaction { tx =>
                
                    tx.execute( "insert into intransactionspec values(%s, %s)", 123, "test" )
                    usedConnection = tx.connection
                    error( "not again?!" )
                }
            }
            catch {
                case e: RuntimeException => // expected
            }
            
            usedConnection.isClosed should be (true)
        }

    }
}
