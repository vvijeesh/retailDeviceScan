name := "Retail Device Scan"

version := "0.1"

scalaVersion := "2.12.3"

useCoursier := false

// https://mvnrepository.com/artifact/org.springframework.boot/spring-boot-starter-web
libraryDependencies += "org.springframework.boot" % "spring-boot-starter-web" % "2.3.7.RELEASE"
libraryDependencies += "org.springframework.boot" % "spring-boot-starter" % "2.3.7.RELEASE"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
