package com.devmedia.transformation;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Exemplo3 {

      public static void main(String[] args) {

// configuração do Spark
            SparkConf conf = new SparkConf().setMaster("local").setAppName("BusProcessor");
            JavaSparkContext ctx = new JavaSparkContext(conf);

// carrega os dados dos ônibus de sp
            JavaRDD<String> linhas = ctx.textFile("c:/dev/teste7.log");

// filtra os registros de ônibus pelo nome da linha
            JavaRDD<String> linhasFiltradas = linhas.filter(s -> s.contains("JD.BONFIGLIOLI"));

// mostra todos os ônibus filtrados
            List<String> resultados = linhasFiltradas.collect();
            for (String linha : resultados) {
                  System.out.println(linha);
            }

            ctx.close();

      }

}