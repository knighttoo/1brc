/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collector;

import static java.util.stream.Collectors.groupingBy;

public class CalculateAverage_sunshine {

    private static final String FILE = "./measurements.txt";

    private static record Measurement(String station, double value) {
        private Measurement(String[] parts) {
            this(parts[0], Double.parseDouble(parts[1]));
        }
    }

    private static record ResultRow(double min, double mean, double max) {
        public String toString() {
            return round(min) + "/" + round(mean) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    };

    private static class MeasurementAggregator {
        private double min = Double.POSITIVE_INFINITY;
        private double max = Double.NEGATIVE_INFINITY;
        private double sum;
        private long count;
    }

    public static void main(String[] args) throws IOException {
        // Map<String, Double> measurements1 = Files.lines(Paths.get(FILE))
        // .map(l -> l.split(";"))
        // .collect(groupingBy(m -> m[0], averagingDouble(m -> Double.parseDouble(m[1]))));
        //
        // measurements1 = new TreeMap<>(measurements1.entrySet()
        // .stream()
        // .collect(toMap(e -> e.getKey(), e -> Math.round(e.getValue() * 10.0) / 10.0)));
        // System.out.println(measurements1);

        // stream parallelism X
        // stream buffer input X
        // chuck out the lines
        // move the work and remove news

        Map<String, MeasurementAggregator> measrements = new ConcurrentHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(Paths.get(FILE).toFile()))) {
            reader.lines().parallel().forEach(line -> {
                String[] parts = line.split(";");
                Measurement measurement = new Measurement(parts);
                measrements.compute(measurement.station(), (station, aggregator) -> {
                    if (aggregator == null) {
                        aggregator = new MeasurementAggregator();
                    }
                    aggregator.min = Math.min(aggregator.min, measurement.value);
                    aggregator.max = Math.max(aggregator.max, measurement.value);
                    aggregator.sum += measurement.value;
                    aggregator.count++;
                    return aggregator;
                });
            });
        }

        Map<String, String> resultsOf = new TreeMap<>();
        for (Map.Entry<String, MeasurementAggregator> entryOf : measrements.entrySet()) {
            MeasurementAggregator aggregator = entryOf.getValue();
            resultsOf.put(entryOf.getKey(), new ResultRow(aggregator.min, aggregator.sum / aggregator.count, aggregator.max).toString());
        }
        System.out.println(resultsOf);

        /*
         * Collector<Measurement, MeasurementAggregator, ResultRow> collector = Collector.of(
         * MeasurementAggregator::new,
         * (a, m) -> {
         * a.min = Math.min(a.min, m.value);
         * a.max = Math.max(a.max, m.value);
         * a.sum += m.value;
         * a.count++;
         * },
         * (agg1, agg2) -> {
         * var res = new MeasurementAggregator();
         * res.min = Math.min(agg1.min, agg2.min);
         * res.max = Math.max(agg1.max, agg2.max);
         * res.sum = agg1.sum + agg2.sum;
         * res.count = agg1.count + agg2.count;
         * 
         * return res;
         * },
         * agg -> {
         * return new ResultRow(agg.min, agg.sum / agg.count, agg.max);
         * });
         * 
         * Map<String, ResultRow> measurements = new TreeMap<>(Files.lines(Paths.get(FILE))
         * .map(l -> new Measurement(l.split(";")))
         * .collect(groupingBy(m -> m.station(), collector)));
         * 
         * System.out.println(measurements);
         */
    }
}
