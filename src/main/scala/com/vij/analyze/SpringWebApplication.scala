package com.vij.analyze

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.{EnableAutoConfiguration, SpringBootApplication}

@EnableAutoConfiguration
@SpringBootApplication
class SpringWebApplication {
    println("\n\n\n *** Hello World..... Starting Main SBApp ***\n\n\n")
}

object SpringWebApplication extends App {
    SpringApplication.run(classOf[SpringWebApplication])
}
