package org.example;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartFrame;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.labels.BoxAndWhiskerToolTipGenerator;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.category.BoxAndWhiskerRenderer;
import org.jfree.data.category.DefaultCategoryDataset;
import org.jfree.data.statistics.DefaultBoxAndWhiskerCategoryDataset;

import java.awt.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("Main").master("local[*]").getOrCreate();
        session.sparkContext().setLogLevel("ERROR");

        Dataset<Row> dataSet = session.sqlContext().read().format("csv").option("header", "true").load("C:\\Users\\otavio_a_santos\\IdeaProjects\\learningSparkDS\\testeSpark\\src\\main\\java\\org\\example\\telecom_users.csv");

        dataSet.show();

        // Remove colunas que não serão utilizadas
        dataSet = dataSet.drop("_c0");

        dataSet.show();

        // Mostra todos os tipos de dados existentes na coluna TipoContrato e a quantidade de cada um
        dataSet.groupBy("TipoContrato").count().show();

        // Mostra todos os tipos de dados existentes na coluna TipoContrato e a quantidade de cada um em forma de gráfico de barra
        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        dataset.addValue(dataSet.filter(dataSet.col("TipoContrato").equalTo("Mensal")).count(), "Mensal", "Mensal");
        dataset.addValue(dataSet.filter(dataSet.col("TipoContrato").equalTo("Anual")).count(), "Anual", "Anual");
        dataset.addValue(dataSet.filter(dataSet.col("TipoContrato").equalTo("2 anos")).count(), "2 anos", "2 anos");

        JFreeChart chart = ChartFactory.createBarChart(
                "Tipos de Contrato",
                "Tipo de Contrato",
                "Quantidade",
                dataset,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );
        ChartFrame frame = new ChartFrame("Tipos de Contrato", chart);
        frame.pack();
        frame.setVisible(true);

        // Agora com a coluna MesesComoCliente
        dataSet.groupBy("MesesComoCliente").count().show();
        DefaultCategoryDataset dataset2 = new DefaultCategoryDataset();

        for (Row row : dataSet.groupBy("MesesComoCliente").count().collectAsList()) {
            dataset2.addValue(row.getLong(1), "MesesComoCliente", row.getString(0));
        }

        JFreeChart chart2 = ChartFactory.createBarChart(
                "Meses como Cliente",
                "Meses como Cliente",
                "Quantidade",
                dataset2,
                PlotOrientation.VERTICAL,
                true,
                true,
                false
        );
        ChartFrame frame2 = new ChartFrame("Meses como Cliente", chart2);
        frame2.pack();
        frame2.setVisible(true);

        // Agora mostrar todos os aposentados
        dataSet.filter(dataSet.col("Aposentado").equalTo("1")).show();

        // Gráfico Boxplot dos aposentados e tipo de contrato
        Dataset<Row> filteredData = dataSet.filter("Aposentado = '1'");

        // Criar o dataset para o gráfico de boxplot
        DefaultBoxAndWhiskerCategoryDataset dataset3 = new DefaultBoxAndWhiskerCategoryDataset();

        // Agrupar os dados por tipo de contrato
        Dataset<Row> counts = filteredData.groupBy("TipoContrato").count();

        // Criar um mapa para armazenar os valores por tipo de contrato
        Map<String, List<Double>> valuesMap = new HashMap<>();

        for (Row row : counts.collectAsList()) {
            String tipoContrato = row.getString(0);
            Long count = row.getLong(1);

            // Filtrar os dados por tipo de contrato
            Dataset<Row> filtered = filteredData.filter("TipoContrato = '" + tipoContrato + "'");

            // Converter o campo "MesesComoCliente" para o tipo Double
            Dataset<Double> doubleValues = filtered.select(filtered.col("MesesComoCliente").cast("double")).as(Encoders.DOUBLE());

            // Adicionar os valores ao mapa
            valuesMap.put(tipoContrato, doubleValues.collectAsList());
        }

        // Calcular os quartis e demais estatísticas para cada tipo de contrato
        for (String tipoContrato : valuesMap.keySet()) {
            List<Double> values = valuesMap.get(tipoContrato);

            Collections.sort(values);

            //calcular em um intervalo válido (0, 100)
            double q1 = values.get((int) Math.round(values.size() * 0.25));
            double q2 = values.get((int) Math.round(values.size() * 0.50));
            double q3 = values.get((int) Math.round(values.size() * 0.75));
            double min = values.get(0);
            double max = values.get(values.size() - 1);

            // Adicionar os valores ao dataset
            dataset3.add(values, tipoContrato, tipoContrato);

        }


        // Criar o gráfico de boxplot
        JFreeChart chart3 = ChartFactory.createBoxAndWhiskerChart(
                "Meses como Cliente",
                "Tipo de Contrato",
                "Meses como Cliente",
                dataset3,
                true
        );

        // Adicionar as estatísticas no gráfico
        BoxAndWhiskerRenderer renderer = new BoxAndWhiskerRenderer();
        renderer.setFillBox(false);
        renderer.setMeanVisible(false);
        renderer.setMedianVisible(true);
        renderer.setSeriesPaint(0, Color.BLACK);
        renderer.setSeriesOutlinePaint(0, Color.BLACK);
        renderer.setSeriesToolTipGenerator(0, new BoxAndWhiskerToolTipGenerator());
        CategoryPlot plot = chart3.getCategoryPlot();
        plot.setRenderer(renderer);

        // Exibir o gráfico
        ChartFrame frame3 = new ChartFrame("Meses como Cliente", chart3);
        frame3.pack();
        frame3.setVisible(true);


    }
}
