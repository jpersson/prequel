package net.noerd.prequel

trait Formattable {
    def escaped( formatter: SQLFormatter ): String
    def value: Any
}