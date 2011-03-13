package net.noerd.prequel

import java.util.Date

import org.scalatest.Spec
import org.scalatest.matchers.ShouldMatchers

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.joda.time.format.DateTimeFormatter

class SQLFormatterSpec extends Spec with ShouldMatchers {
    
    val hsqldbFormatter = DateTimeFormat.forPattern( "yyyy-MM-dd HH:mm:ss" )
    val sqlFormatter = SQLFormatter( hsqldbFormatter )
    
    def format( sql: String, params: Any* ) = sqlFormatter.format( sql, params.toSeq )

    describe( "SQL" ) {
        
        it( "should format DateTime params") {
            val dateTime: DateTime = hsqldbFormatter.parseDateTime( "2010-03-13 13:00:00" )
            val expected = "insert into foo values('2010-03-13 13:00:00')"
            val actual = format( "insert into foo values(%s)", dateTime )
            
            actual should equal (expected)
        }
        
        it( "should format Date params") {
            val date: Date = hsqldbFormatter.parseDateTime( "2010-03-13 13:00:00" ).toDate
            val expected = "insert into foo values('2010-03-13 13:00:00')"
            val actual = format( "insert into foo values(%s)", date )
            
            actual should equal (expected)
        }

        it( "should format NullComparable params") {
            
            val expectedNone = "select from foo where bar is null"
            val actualNone = format( "select from foo where bar %s", NullComparable( None ) )
            
            actualNone should equal (expectedNone)
            
            val expectedSome = "select * from foo where bar ='foobar'"
            val actualSome = format( "select * from foo where bar %s", NullComparable( Some("foobar") ) )
            
            actualSome should equal (expectedSome)
        }        

        it( "should format Nullable params") {
            
            val expectedNone = "insert into foo values(null)"
            val actualNone = format( "insert into foo values(%s)", Nullable( None ) )
            
            actualNone should equal (expectedNone)
            
            val expectedSome = "insert into foo values('bar')"
            val actualSome = format( "insert into foo values(%s)", Nullable( Some("bar") ) )
            
            actualSome should equal (expectedSome)
        }        

        it( "should format Identifier params") {
            
            val expected = "insert into foo.bar values('test')"
            val actual = format( "insert into %s values(%s)", Identifier( "foo.bar" ), "test" )
            
            actual should equal (expected)
        }

        it( "should format String params" ) {
            
            val expected = "select * from foo where bar = 'test'"
            val actual = format( "select * from foo where bar = %s", "test" )
            
            actual should equal (expected)
        }
        
        it( "should format Long params" ) {
            
            val expected = "select * from foo where bar = 123456"
            val actual = format( "select * from foo where bar = %s", 123456L )
            
            actual should equal (expected)
        }

        it( "should format Int params" ) {
            
            val expected = "select * from foo where bar = 123456"
            val actual = format( "select * from foo where bar = %s", 123456 )
            
            actual should equal (expected)
        }

        it( "should format Float params" ) {
            
            val expected = "select * from foo where bar = 1.500000"
            val actual = format( "select * from foo where bar = %s", 1.5F )
            
            actual should equal (expected)
        }

    }
}
