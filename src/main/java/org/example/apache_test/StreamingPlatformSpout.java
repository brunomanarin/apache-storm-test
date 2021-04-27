package org.example.apache_test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class StreamingPlatformSpout implements IRichSpout {

 /**
  * 
  */
 private SpoutOutputCollector collector;
 private boolean isCompleted = false;
 private FileReader fileReader;
 private BufferedReader reader;
 private String str;
 String fileName;

 public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
  try {
	this.fileReader = new FileReader(conf.get("inputFile").toString());  
  } catch (FileNotFoundException e) {
	  throw new RuntimeException("Error reading file");
  }
  this.collector = collector;
  this.reader = new BufferedReader(fileReader);
 }

 public void close() {
  // TODO Auto-generated method stub

 }

 public void activate() {
  // TODO Auto-generated method stub

 }


 public void deactivate() {
  // TODO Auto-generated method stub

 }

 public void nextTuple() {
  if (!isCompleted) {
	  try {
		  this.str = reader.readLine();
		   if(this.str != null) {
			   this.collector.emit(new Values(str));
		   } else {
			  isCompleted = true;
			  fileReader.close();
		   }
	  } catch (Exception e) {
		  throw new RuntimeException("Error reading tuple", e);
	  }
   
  } else {
   this.close();
  }
 }

 public void ack(Object msgId) {
  // TODO Auto-generated method stub

 }

 public void fail(Object msgId) {
  // TODO Auto-generated method stub

 }

 public void declareOutputFields(OutputFieldsDeclarer declarer) {
  declarer.declare(new Fields("csvRow"));

 }

 public Map < String, Object > getComponentConfiguration() {
  // TODO Auto-generated method stub
  return null;
 }

}