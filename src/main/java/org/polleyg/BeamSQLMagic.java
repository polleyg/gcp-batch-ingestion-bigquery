package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;

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
        pipeline.apply("file_read", TextIO.read().from("gs://batch-pipeline-sql/input.csv"))
                .apply("row_transform", ParDo.of(new RowParDo())).setRowSchema(SCHEMA)
                .apply("sql_transform", SqlTransform.query("SELECT lang, SUM(views) FROM PCOLLECTION GROUP BY lang"))
                .apply("to_bq_row_transform", ParDo.of(new RowToBigQueryRow()))
                .apply("results_write", BigQueryIO.writeTableRows()
                        .to(String.format("%s:beam_sql.sql_output", options.getProject()))
                        .withCreateDisposition(CREATE_IF_NEEDED)
                        .withWriteDisposition(WRITE_TRUNCATE)
                        .withSchema(getTableSchemaAggregated()));
        pipeline.run();
    }

    private static TableSchema getTableSchemaAggregated() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("lang").setType("STRING"));
        fields.add(new TableFieldSchema().setName("views").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

    //ParDo for String -> Row (SQL)
    public static class RowParDo extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(HEADER)) {
                String[] vals = c.element().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                Row appRow = Row
                        .withSchema(SCHEMA)
                        .addValues(vals[4], Integer.valueOf(vals[6]))
                        .build();
                c.output(appRow);
            }
        }
    }

    //ParDo for Row (SQL) -> TableRow
    public static class RowToBigQueryRow extends DoFn<Row, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            TableRow bqRow = new TableRow()
                    .set("lang", c.element().getString("lang"))
                    .set("views", c.element().getInt32("EXPR$1")); //Beam bug?
            c.output(bqRow);
        }
    }
}
