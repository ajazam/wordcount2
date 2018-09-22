# wordcount2
Distributed word count using akka and java 
Tradeoffs
  * one master, multiple workers.
  * Master not fault tolerant, graceful recovery from worker failure.
  * Words read from XML file.
  * Final result will shown in the console / website (time permitting).

The project is based on a heavily trimmed [akka cluster for Java](https://developer.lightbend.com/guides/akka-sample-cluster-java) example.


To run the wordcount the jar file needs to be stored on the master and worker nodes


To run on the master node type
java -Dakka.cluster.roles.0=master -Dakka.cluster.seed-nodes.0="akka://WordCountSystem@172.16.0.18:2551" -Dhostip="172.16.0.18" -Dfile.name=README.md -jar wordcount2-assembly-0.1.0-SNAPSHOT.jar -Xss10M
where hostip is the IP of the host the software is running on. The IP in akka://WordCountSystem@172.16.0.18:2551 is the IP of the master.


To run on a worker node type
java -Dakka.cluster.roles.0=worker -Dakka.cluster.seed-nodes.0="akka://WordCountSystem@172.16.0.18:2551" -Dhostip="172.16.0.5" -jar wordcount2-assembly-0.1.0-SNAPSHOT.jar
where hostip is the IP of the host the software is running on. The IP in akka://WordCountSystem@172.16.0.18:2551 is the IP of the master.

In the both terminal sessions messages about nodes moving UP can be seen both for the master and worker IP addresses.
