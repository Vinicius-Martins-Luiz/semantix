package EngenheiroDadosViniciusMartinsLuiz;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class App {

	public static void main(String[] args) {
		
		// Buscar um PATH relativo à máquina de quem baixou esse código
		Path p = Paths.get(System.getProperty("user.dir"));
		
		// Configura o Logger do Log4J para não mostrar todas as mensagens de info padrão dos pacotes
		// do namespace "org" (nesse meu exemplo, basicamente o SPARK)
		Logger.getLogger("org").setLevel(Level.OFF);
		
		// Cria um conf do Spark rodando local e usando a quantidade de cores disponiveis no processador
        SparkConf conf = new SparkConf()
	        	.setAppName("count")
	        	.setMaster("local[*]");

        // Cria um Spark Context do Java
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Carrega os dois arquivos para um RDD, depois une os dois em um unico RDD
        JavaRDD<String> rddJul = jsc.textFile(p + "\\ArquivosNasa\\NASA_access_log_Jul95.gz");
        JavaRDD<String> rddAgo = jsc.textFile(p + "\\ArquivosNasa\\NASA_access_log_Aug95.gz");
        JavaRDD<String> rddUnion = rddJul.union(rddAgo).filter( linha -> linha.contains(" - - "));
        
        // REGEX PARA TOKENIZAR AS LINHAS DO ARQUIVO
        String regex = "(((\\s\\-){2}\\s\\[)|(\\]\\s\\\")|(\\\"\\s(?=\\d))|(\\s(?=\\d+$))|(\\s(?=\\-$)))";
        
        
        // ================= RDD PARA LISTA DE HOSTS UNICOS ================= 
        JavaPairRDD<String, Integer> hostsUnicos = rddUnion.mapToPair(
	    	(PairFunction<String, String, Integer>) s -> {
	    		return new Tuple2<String, Integer>( s.split(regex)[0], 1 );
	    	}
		);

        // ================= RDD PARA TOTAL DE ERROS 404 ================= 
        JavaPairRDD<String, Integer> totalErros404 = rddUnion.mapToPair(
            	(PairFunction<String, String, Integer>) s -> {
            		return new Tuple2<String, Integer>( s.split(regex)[3], 1 );
            	}
		).filter( s -> s._1().startsWith("404"));
        
        
        // ================= RDD PARA TOP5 DE HOSTS COM ERROS 404 =================         
        JavaPairRDD<String, Integer> top5URL = rddUnion.mapToPair(
            	(PairFunction<String, String, String>) s -> {
            		return new Tuple2<String, String>( s.split(regex)[0], s.split(regex)[3]);
            	}
		)
		.filter( s -> s._2().startsWith("404"))     
		.mapToPair(
            	(PairFunction<Tuple2<String, String>, String, Integer>) s -> {
            		return new Tuple2<String, Integer>( s._1(), 1 );
            	}
		);
        
        // ================= RDD PARA ERROS 404 POR DIA ================= 
        JavaPairRDD<String, Integer> erros404porDia = rddUnion.mapToPair(
            	(PairFunction<String, String, String>) s -> {
            		return new Tuple2<String, String>( s.split(regex)[1].substring(0, 2), s.split(regex)[3]);
            	}
		)
		.filter( s -> s._2().startsWith("404"))     
		.mapToPair(
            	(PairFunction<Tuple2<String, String>, String, Integer>) s -> {
            		return new Tuple2<String, Integer>( s._1(), 1 );
            	}
		);
        
        // ================= RDD PARA TOTAL DE BYTES RETORNADOS ================= 
        JavaRDD<Integer> totalBytes = rddUnion.map( s -> Integer.parseInt(s.split(regex)[4].replace("-", "0")));
        
   
       
        
        // ======================  OUTPUT DOS VALORES ============================== //       
        System.out.println("\nTotal Hosts Unicos\n" +	hostsUnicos.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b).count());
        System.out.println("\nTotal Erros 404\n" + totalErros404.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b).first()._2());

        
                
        List<Tuple2<Integer, String>> topErros404 = top5URL
        		.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b)
        		.mapToPair( s -> s.swap())
        		.sortByKey(false)
        		.take(5);
        
        System.out.println("\nTop 5 URLs com + Erros 404\n");
        for (Tuple2<Integer, String> item : topErros404) {
			System.out.println("URL: " + item._2() + " -> Total:" + item._1());
		}
       
        

        List<Tuple2<String, Integer>> erros404dia = erros404porDia
        		.reduceByKey((Function2<Integer, Integer, Integer>) (a, b) -> a + b)
        		.sortByKey()
        		.collect();
        System.out.println("\nQuantidade erros 404 por dia\n");
        for (Tuple2<String, Integer> item : erros404dia) {
			System.out.println(item._1() + " -> Total:" + item._2());
		}
        
        System.out.println("\nTotal BYTES retornados\n" + totalBytes.reduce((Function2<Integer, Integer, Integer>) (a,b) -> a + b));

	}

}
