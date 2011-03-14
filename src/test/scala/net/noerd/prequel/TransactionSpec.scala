package net.noerd.prequel

import java.sql.SQLException

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach

class TransactionSpec extends Spec with ShouldMatchers with BeforeAndAfterEach {
    
    implicit val databaseConfig = TestDatabase.config
    
    override def beforeEach() = InTransaction { tx =>
        tx.execute( "create table transactionspec(id int, name varchar(265))" )
        tx.execute( "insert into transactionspec values(%s, %s)", 242, "test1" )
        tx.execute( "insert into transactionspec values(%s, %s)", 23, "test2" )
        tx.execute( "insert into transactionspec values(%s, %s)", 42, "test3" )
    }
    
    override def afterEach() = InTransaction { tx =>
        tx.execute( "drop table transactionspec" )
    }    

    describe( "Transaction" ) {
        
        describe( "select" ) {
            
            it( "should return a Seq of the records converted by block" ) {
                InTransaction { tx =>
                    val expected = Seq( "test1", "test2", "test3" )
                    val actual = tx.select("select name from transactionspec") { row =>
                        row.nextString
                    }
                    
                    actual should equal (expected) 
                }
            }
            
            it( "should return an empty Seq if no records were found" ) {
                InTransaction { tx =>
                    val actual = tx.select(
                        """select name from transactionspec 
                            where id > 1000
                        """
                    ) { _.nextString }
                
                    actual should be ('empty) 
                }
            }
        }

        describe( "selectHead" ) {
            
            it( "should return the first record if one or more is returned by query" ) {
                InTransaction { tx =>
                    val expected = Some( "test1" )
                    val actual = tx.selectHead("select name from transactionspec") { row =>
                        row.nextString
                    }
                    
                    actual should equal (expected)
                }
            }
            
            it( "should return None if the query did not return any records" ) {
                InTransaction { tx =>
                    val expected = None
                    val actual = tx.selectHead(
                        """select name from transactionspec 
                            where id > 1000
                        """
                    ) { _.nextString }
                    
                    actual should equal (expected)
                }                
            }
        }

        describe( "selectLong" ) {
            
            it( "should return the first column of the first records as a Long" ) {
                InTransaction { tx =>
                    val expected = 242
                    val actual = tx.selectLong("select id from transactionspec")
                    
                    actual should equal (expected) 
                }                
            }
            
            it( "should throw an SQLException if the value is not a Long" ) {
                InTransaction { tx =>
                    try {
                        tx.selectLong("select 'nan' from transactionspec")
                        error( "this should not execute" )
                    }
                    catch {
                        case e: SQLException => // Expected
                    }
                }                                
            }

            it( "should throw a NoSuchElementException if no record was returned" ) {
                InTransaction { tx =>
                    try {
                        tx.selectLong(
                            """select id from transactionspec 
                                where id > 1000
                            """
                        )
                        error( "this should not execute" )
                    }
                    catch {
                        case e: NoSuchElementException => // Expected
                    }
                }
            }
        }
    }
}