package com.vij.analyze

import org.springframework.web.bind.annotation.{RequestMapping, RestController}

@RestController
class ScanController {
  @RequestMapping(value = Array("/hello"))
  def getTarget(aa: String) = { println("F inside Controller is "+aa) }
  getTarget("Duplicate")
}
