package net.noerd.prequel

trait Formattable {
    def escaped( formatter: SQLFormatter ): String
    def addTo( statement: RichPreparedStatement ): Unit
    def value: Any
}