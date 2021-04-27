package org.example.apache_test;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.File;   

public class StreamingPlatformBolt implements IRichBolt {

 /**
  * 
  */
 Map < Integer, Integer > counterPerYear;
 Map < String, Integer > counterTargetAge;
 
 Integer netflixCounterKids = 0;
 Integer huluCounterKids = 0;
 Integer primeCounterKids = 0;
 Integer disneyCounterKids = 0;
 
 Integer netflixCounterTvShows = 0;
 Integer huluCounterTvShows = 0;
 Integer primeCounterTvShows = 0;
 Integer disneyCounterTvShows = 0;
 
 Integer netflixCounterTopTvShows = 0;
 Integer huluCounterTopTvShows = 0;
 Integer primeCounterTopTvShows = 0;
 Integer disneyCounterTopTvShows = 0;
 
 String mostChildrenFriendly;
 
 Integer id;
 String name;
 Integer entryCounter = 0;

 Map<String, Object> car = new HashMap<>();
 
 File output;
 String processedFileName;
 
 public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
  this.counterPerYear = new HashMap <> ();
  this.counterTargetAge = new HashMap <> ();
  this.name = context.getThisComponentId();
  this.id = context.getThisTaskId();
  this.output = new File(stormConf.get("outputFile").toString());
  this.processedFileName = stormConf.get("inputFile").toString();
 }
 


 public void execute(Tuple input) {
  String word = input.getStringByField("csvRow");
  String [] csvRowSplit = word.split(",");
  
  String tvShowTitle = csvRowSplit[1];
  String yearOfRelease = csvRowSplit[2];
  try {
	  String targetAge = "";
	  if(!csvRowSplit[3].equals("")) {
		  if(csvRowSplit[3].equals("all")) {
			  targetAge = "0";
		  } else if(csvRowSplit[3].length()>2) {
			  targetAge = csvRowSplit[3].substring(0,2);
		  } else {
			  targetAge = csvRowSplit[3].substring(0,1);
		  }
		  
		  try {
			  Integer.parseInt(targetAge);
		  } catch(Exception e) {
			  targetAge = "999";
		  }
	  }
	  
	 
	  Double imdbScore;
	  try {
		  if(!csvRowSplit[4].equals("")) {
			  imdbScore = Double.parseDouble(csvRowSplit[4]);
		  } else {
			  imdbScore = 0.0;
		  }
	  }catch(Exception e) {
		  imdbScore = 0.0;
	  }
	  String rottenTomatoesScore = csvRowSplit[5];
	  int isOnNetflix = Integer.parseInt(csvRowSplit[6]);
	  int isOnHulu = Integer.parseInt(csvRowSplit[7]);
	  int isOnPrime = Integer.parseInt(csvRowSplit[8]);
	  int isOnDisney = Integer.parseInt(csvRowSplit[9]);

	  
	  if(isOnNetflix == 1) {
		  netflixCounterTvShows++;
	  }
	  if(isOnHulu == 1) {
		  huluCounterTvShows++;
	  }
	  if(isOnPrime == 1) {
		  primeCounterTvShows++;
	  }
	  if(isOnDisney == 1) {
		  disneyCounterTvShows++;
	  }
	  
	  if(Integer.parseInt(targetAge)<16) {
		  if(isOnNetflix == 1) {
			  netflixCounterKids++;
		  }
		  if(isOnHulu == 1) {
			  huluCounterKids++;
		  }
		  if(isOnPrime == 1) {
			  primeCounterKids++;
		  }
		  if(isOnDisney == 1) {
			  disneyCounterKids++;
		  }
	  }
	  
	  if(imdbScore>8.0) {
		  if(isOnNetflix == 1) {
			  netflixCounterTopTvShows++;
		  }
		  if(isOnHulu == 1) {
			  huluCounterTopTvShows++;
		  }
		  if(isOnPrime == 1) {
			  primeCounterTopTvShows++;
		  }
		  if(isOnDisney == 1) {
			  disneyCounterTopTvShows++;
		  }
	  }

  } catch(Exception e) {
	  System.out.println(tvShowTitle);
	  System.out.println(e);
  }
  
  int entryContainsYear;
  
  try {
	  entryContainsYear = Integer.parseInt(yearOfRelease);
  }
  catch (NumberFormatException e)
  {
	  entryContainsYear = 0;
  }
  
  //count the amount of entries per year
  if(!counterPerYear.containsKey(entryContainsYear)){
	  counterPerYear.put(entryContainsYear, 1);
  }else {
      Integer c = counterPerYear.get(entryContainsYear) + 1;
      counterPerYear.put(entryContainsYear, c);
  }
  
  entryCounter++;
  
 }


 public void cleanup() {
	System.out.println("----------------------------------------------------");
	System.out.println("INFORMAÇÕES SOBRE O ARQUIVO PROCESSADO: " + this.processedFileName);
	System.out.println(" ");
	System.out.println("Total de filmes na lista: " + entryCounter.toString());
	System.out.println(" ");
	System.out.println("Plataforma com mais programas de TV:");
	System.out.println("Netflix: " + netflixCounterTvShows.toString());
	System.out.println("Hulu: " + huluCounterTvShows.toString());
	System.out.println("Amazon Prime: " + primeCounterTvShows.toString());
	System.out.println("Disney Plus: " + disneyCounterTvShows.toString());
	System.out.println("Plataforma com os melhores programas de TV segundo o IMDb (nota acima de 8):");
	System.out.println("Netflix: " + netflixCounterTopTvShows.toString());
	System.out.println("Hulu: " + huluCounterTopTvShows.toString());
	System.out.println("Amazon Prime: " + primeCounterTopTvShows.toString());
	System.out.println("Disney Plus: " + disneyCounterTopTvShows.toString());
	System.out.println("Plataforma com mais conteúdo para crianças:");
	System.out.println("Netflix: " + netflixCounterKids.toString());
	System.out.println("Hulu: " + huluCounterKids.toString());
	System.out.println("Amazon Prime: " + primeCounterKids.toString());
	System.out.println("Disney Plus: " + disneyCounterKids.toString());

//    for(Map.Entry<Integer, Integer> entry : counterPerYear.entrySet()){
//        System.out.println(entry.getKey()+": "+entry.getValue());
//    }
    System.out.println("----------------------------------------------------");
 }

 public void declareOutputFields(OutputFieldsDeclarer declarer) {
  // TODO Auto-generated method stub

 }

 public Map < String, Object > getComponentConfiguration() {
  // TODO Auto-generated method stub
  return null;
 }

}