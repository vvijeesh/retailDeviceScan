package com.vij.analyze

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer
import org.springframework.context.annotation.ComponentScan

@EnableAutoConfiguration
@SpringBootApplication
class SpringWebApplication {
    println("\n\n\n *** Hello World..... Starting Main SB App ***\n\n\n")
}

//Start a Springboot Web Application
object SpringWebApp extends App {
    SpringApplication.run(classOf[SpringWebApplication])
}
