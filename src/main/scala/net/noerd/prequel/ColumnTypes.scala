package net.noerd.prequel

import java.util.Date

import org.joda.time.DateTime
import org.joda.time.Duration
import org.joda.time.format.DateTimeFormatter

//
// String
//
class StringColumnType( row: ResultSetRow ) extends ColumnType[ String ] {
    override def nextValueOption: Option[ String ] = row.nextString
}
object StringColumnType extends ColumnTypeFactory[ String ] {
    def apply( row: ResultSetRow ) = new StringColumnType( row )
}

//
// Boolean
// 
class BooleanColumnType( row: ResultSetRow ) extends ColumnType[ Boolean ] {
    override def nextValueOption: Option[ Boolean ] = row.nextBoolean
}
object BooleanColumnType extends ColumnTypeFactory[ Boolean ] {
    def apply( row: ResultSetRow ) = new BooleanColumnType( row )
}

//
// Long
//
class LongColumnType( row: ResultSetRow ) extends ColumnType[ Long ] {
    override def nextValueOption: Option[ Long ] = row.nextLong
}
object LongColumnType extends ColumnTypeFactory[ Long ] {
    def apply( row: ResultSetRow ) = new LongColumnType( row )
}

//
// Int
//
class IntColumnType( row: ResultSetRow ) extends ColumnType[ Int ] {
    override def nextValueOption: Option[ Int ] = row.nextInt
}
object IntColumnType extends ColumnTypeFactory[ Int ] {
    def apply( row: ResultSetRow ) = new IntColumnType( row )
}

//
// Float
//
class FloatColumnType( row: ResultSetRow ) extends ColumnType[ Float ] {
    override def nextValueOption: Option[ Float ] = row.nextFloat
}
object FloatColumnType extends ColumnTypeFactory[ Float ] {
    def apply( row: ResultSetRow ) = new FloatColumnType( row )
}

//
// Double
//
class DoubleColumnType( row: ResultSetRow ) extends ColumnType[ Double ] {
    override def nextValueOption: Option[ Double ] = row.nextDouble
}
object DoubleColumnType extends ColumnTypeFactory[ Double ] {
    def apply( row: ResultSetRow ) = new DoubleColumnType( row )
}

//
// DateTime
//
class DateTimeColumnType( row: ResultSetRow ) extends ColumnType[ DateTime ] {
    override def nextValueOption: Option[ DateTime ] = row.nextDate.map( d => new DateTime( d.getTime ) )
}
object DateTimeColumnType extends ColumnTypeFactory[ DateTime ] {
    def apply( row: ResultSetRow ) = new DateTimeColumnType( row )
}
class DateColumnType( row: ResultSetRow ) extends ColumnType[ Date ] {
    override def nextValueOption: Option[ Date ] = row.nextDate
}
object DateColumnType extends ColumnTypeFactory[ Date ] {
    def apply( row: ResultSetRow ) = new DateColumnType( row )
}

//
// Duration
//
class DurationColumnType( row: ResultSetRow ) extends ColumnType[ Duration ] {
    override def nextValueOption: Option[ Duration ] = row.nextLong.map( new Duration( _ ) )
}
object DurationColumnType extends ColumnTypeFactory[ Duration ] {
    def apply( row: ResultSetRow ) = new DurationColumnType( row )
}