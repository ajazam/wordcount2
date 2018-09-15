# wordcount2
Distributed word count using akka and java 
Tradeoffs
  * one master, multiple workers.
  * Master not fault tolerant, graceful recovery from worker failure.
  * Words read from XML file.
  * Final result will shown in console / website (time permitting).
