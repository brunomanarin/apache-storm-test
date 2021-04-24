package org.example.apache_test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class WordCountBolt implements IRichBolt {

 /**
  * 
  */
 Map < String, Integer > counters;
 Integer id;
 String name;
 String fileName;
 Integer counter = 0;

 public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
  this.counters = new HashMap < > ();
  this.name = context.getThisComponentId();
  this.id = context.getThisTaskId();

 }

 public void execute(Tuple input) {
  String word = input.getStringByField("word");
  String [] csvRow = word.split(",");
  System.out.println(csvRow[1]);
  System.out.println(csvRow[2]);
  int foo;
  try {
     foo =   Integer.parseInt(csvRow[2]);
  }
  catch (NumberFormatException e)
  {
     foo = 0;
  }
  if(foo == 2018) {
	  counter++;
  }
  counters.put(csvRow[1],foo);
 }


 public void cleanup() {
  System.out.println("eaeeeeeee man, o total de filmes: " + counter.toString());
//  for (Map.Entry < String,Integer > entry: counters.entrySet()) {
//   System.out.println(entry.getKey());
//  }
 }

 public void declareOutputFields(OutputFieldsDeclarer declarer) {
  // TODO Auto-generated method stub

 }

 public Map < String, Object > getComponentConfiguration() {
  // TODO Auto-generated method stub
  return null;
 }

}