package org.polleyg;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

/**
 * Do some randomness
 */
public class BeamSQLMagic {
    public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";
    public static final Schema SCHEMA = Schema.builder()
            .addStringField("lang")
            .addInt32Field("views")
            .build();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        pipeline.apply("read_from_gcs", TextIO.read().from("gs://batch-pipeline-sql/input/*"))
                .apply("transform_to_row", ParDo.of(new RowParDo())).setRowSchema(SCHEMA)
                .apply("transform_sql", SqlTransform.query("SELECT SUM(sum_views) as total_views FROM (SELECT lang, SUM(views) as sum_views FROM PCOLLECTION GROUP BY lang)"))
                .apply("transform_to_string", ParDo.of(new RowToString()))
                .apply("write_to_gcs", TextIO.write().to("gs://batch-pipeline-sql/output/result.txt").withoutSharding());
        pipeline.run();
    }

    //ParDo for String -> Row (SQL)
    public static class RowParDo extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(HEADER)) {
                String[] vals = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Row appRow = Row
                        .withSchema(SCHEMA)
                        .addValues(vals[4], Integer.valueOf(vals[6])) //don't do this in prod!
                        .build();
                c.output(appRow);
            }
        }
    }

    //ParDo for Row (SQL) -> String
    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            c.output(c.element().getInt32("total_views").toString());
        }
    }
}
