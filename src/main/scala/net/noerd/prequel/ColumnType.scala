package net.noerd.prequel 

/**
 * Extract a value from a certain column type. Used internally
 * to implement support for custom data types like DateTime and
 * more.
 */
trait ColumnType[ T ] {
    /**
     * @throws RunTimeException if the value was null
     */
    def nextValue: T = nextValueOption.getOrElse( 
        error( "unexpected null value")
    )
    /**
     * To be implemented. Should return Some if the value
     * of the current column is not null, if it is return
     * None.
     *
     * @throws Exception if the column could not be intepreted as
     *         the implementing type.
     */
    def nextValueOption: Option[ T ]
}

trait ColumnTypeFactory[T] {
    def apply( row: ResultSetRow ): ColumnType[T]
}