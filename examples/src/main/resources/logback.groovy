import ch.qos.logback.classic.AsyncAppender

appender("CONSOLE", ConsoleAppender) {
    encoder(PatternLayoutEncoder) {
        pattern = "%date{dd/MM/yyyy} %X{akkaTimestamp} %-5level %logger{36} %thread %X{akkaSource} - %msg%n"
    }
}

appender("ASYNC-CONSOLE", AsyncAppender) {
    appenderRef("CONSOLE")
}

root(INFO, ["ASYNC-CONSOLE"])
