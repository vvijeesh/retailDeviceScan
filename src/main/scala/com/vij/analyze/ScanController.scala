package com.vij.analyze

import org.springframework.web.bind.annotation.{GetMapping, PostMapping, RequestBody, RestController}
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer

@RestController
@PostMapping(path = Array("/publish"))
class ScanController extends SpringBootServletInitializer {

  //Home Page : URL Endpoint "/"
  //If this is included, index.html will not play by default. Type "index.html" manually to redirect
  @GetMapping(path = Array("/"))
  def demo: String = {
    "Welcome to Scan Device Portal home - Use /scanEntry to push JSON data"
  }

  //Post Mapping to publish scan entry json
  @PostMapping(path = Array{"/publish/scanEntry"}, consumes = Array{"application/json"})
  def sendMessageToKafkaTopic(@RequestBody scanEntryData: String): Unit = {

    val scanEntryObj = new ScanEntryProducer()

    //Push to Kafka and get metadata for the successful commit
    val scanEntryMeta: Boolean = scanEntryObj.pushToKafka(scanEntryData)

    if (scanEntryMeta) {println("[INFO] Sent Record to Kafka")}
    else
      {println("[ERROR] Error sending Record to Kafka")}
  }

}