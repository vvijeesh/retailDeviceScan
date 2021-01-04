package com.vij.analyze

import org.springframework.web.bind.annotation.{GetMapping, PostMapping, RequestParam, RestController}
import org.apache.kafka.clients.producer.{ProducerRecord, RecordMetadata}
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer

@RestController
@PostMapping(path = Array("/publish"))
class ScanController extends SpringBootServletInitializer {

  //Home Page : URL Endpoint "/"
  //If this is included, index.html will not play by default. Type "index.html" manually to redirect
  @GetMapping(path = Array("/"))
  def demo: String = {
    "Welcome to Scan Device Portal home."
  }

  //Post Mapping to publish scan entry json
  @PostMapping(path = Array("/publish/{scanEntry}"))
  def sendMessageToKafkaTopic(@RequestParam("scanEntry") scanEntry: ProducerRecord[String, String],
                             @RequestParam("propFileParam") propFile: String): Unit = {
    //Get property file name from args
    val prop = propFile

    val scanEntryObj = new ScanEntryProducer()

    //Push to Kafka and get metadata for the successful commit
    val scanEntryMeta = scanEntryObj.pushToKafka(scanEntry,prop)

  }

}