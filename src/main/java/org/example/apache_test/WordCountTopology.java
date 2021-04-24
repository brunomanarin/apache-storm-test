package org.example.apache_test;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class WordCountTopology {

 public static void main(String args[]) {
  System.out.println("enois");
  TopologyBuilder builder = new TopologyBuilder();
  builder.setSpout("word-reader", new WordReaderSpout());
  builder.setBolt("word-counter", new WordCountBolt(), 1).shuffleGrouping("word-reader");
  Config conf = new Config();
  conf.setDebug(true);
  LocalCluster localCluster = new LocalCluster();
  localCluster.submitTopology("wordcounter-topology", conf, builder.createTopology());
  try {
   Thread.sleep(10000);
   localCluster.shutdown();
  } catch (InterruptedException e) {
   // TODO Auto-generated catch block
   e.printStackTrace();
   localCluster.shutdown();
  }
 }

}