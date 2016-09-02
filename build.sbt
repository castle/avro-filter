name := "AvroFilter"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.8"

scalacOptions += "-deprecation"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.1"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.5.0"

resolvers += Resolver.sonatypeRepo("public")

assemblyJarName in assembly := "avro-filter.jar"