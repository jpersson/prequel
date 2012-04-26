package net.noerd.prequel

import org.joda.time.Duration

import org.scalatest.FunSpec
import org.scalatest.matchers.ShouldMatchers

import net.noerd.prequel.SQLFormatter.DefaultSQLFormatter
import net.noerd.prequel.SQLFormatterImplicits._
import net.noerd.prequel.ResultSetRowImplicits._

class SQLFormatterSpec extends FunSpec with ShouldMatchers {
    
    describe( "SQLFormatter" ) {
        it( "should combine the parameters with the query") {
            val expected = "insert into testtable( c1, c3, c4) values( 234, 'test', 3900000 )"
            val actual = DefaultSQLFormatter.format(
                "insert into ?( c1, c3, c4) values( ?, ?, ? )",
                Identifier( "testtable" ), 234, "test", Duration.standardMinutes( 65 )
            )
            
            actual should equal (expected)
        }
    }
}
