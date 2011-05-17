package net.noerd.prequel

trait Formattable {
    def escaped( formatter: SQLFormatter ): String
    def addTo( statement: ReusableStatement ): Unit
    def value: Any
}