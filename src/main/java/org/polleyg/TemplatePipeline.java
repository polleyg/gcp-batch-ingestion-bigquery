package org.polleyg;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.ArrayList;
import java.util.List;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED;
import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE;

/**
 * Do some randomness
 */
public class TemplatePipeline {
    public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";
    public static final Schema SCHEMA = Schema.builder()
            .addStringField("wikimedia_project")
            .addInt32Field("views")
            .build();

    public static void main(String[] args) {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        DataflowPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(DataflowPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        //READ
        PCollection<String> read = pipeline.apply("gcs_read", TextIO.read().from("gs://batch-pipeline-sql/input.csv"));

        //BigQuery Rows (straight write)
        PCollection<TableRow> bigQueryRows = read.apply("csv_transform",
                ParDo.of(new WikiParDo()));

        //Sql Rows
        PCollection<Row> sqlRows = bigQueryRows.apply("row_transform",
                ParDo.of(new RowParDo()));
        sqlRows.setCoder(RowCoder.of(SCHEMA));

        //More Sql Rows
        PCollection<Row> outputStream =
                sqlRows.setRowSchema(SCHEMA)
                        .apply("sql_transform",
                                SqlTransform.query(
                                        "select wikimedia_project, sum(views) " +
                                                "from PCOLLECTION " +
                                                "group by wikimedia_project"));
        outputStream.setCoder(RowCoder.of(SCHEMA));

        //And back to BigQuery table rows *sigh*
        PCollection<TableRow> sqlAggregated = outputStream.apply("bq_row_transform",
                ParDo.of(new RowToBigQueryRow()));


        bigQueryRows.apply("raw_write_bq", BigQueryIO.writeTableRows()
                .to(String.format("%s:beam_sql.full_output", options.getProject()))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_TRUNCATE)
                .withSchema(getTableSchema()));

        sqlAggregated.apply("aggregated_write_bq", BigQueryIO.writeTableRows()
                .to(String.format("%s:beam_sql.sql_output", options.getProject()))
                .withCreateDisposition(CREATE_IF_NEEDED)
                .withWriteDisposition(WRITE_TRUNCATE)
                .withSchema(getTableSchemaAggregated()));

        pipeline.run();
    }

    private static TableSchema getTableSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("day").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("wikimedia_project").setType("STRING"));
        fields.add(new TableFieldSchema().setName("language").setType("STRING"));
        fields.add(new TableFieldSchema().setName("title").setType("STRING"));
        fields.add(new TableFieldSchema().setName("views").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

    private static TableSchema getTableSchemaAggregated() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("wikimedia_project").setType("STRING"));
        fields.add(new TableFieldSchema().setName("views").setType("INTEGER"));
        return new TableSchema().setFields(fields);
    }

    public static class WikiParDo extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (c.element().equalsIgnoreCase(HEADER)) return;
            String[] split = c.element().split(",");
            if (split.length > 7) return;
            TableRow row = new TableRow();
            for (int i = 0; i < split.length; i++) {
                TableFieldSchema col = getTableSchema().getFields().get(i);
                row.set(col.getName(), split[i]);
            }
            c.output(row);
        }
    }

    public static class RowParDo extends DoFn<TableRow, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Row appRow = Row
                    .withSchema(SCHEMA)
                    .addValues(
                            c.element().get("wikimedia_project"),
                            Integer.valueOf((String) c.element().get("views"))
                    )
                    .build();
            c.output(appRow);
        }
    }

    public static class RowToBigQueryRow extends DoFn<Row, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Row row = c.element();
            TableRow bqRow = new TableRow();

            bqRow.set("wikimedia_project", row.getString("wikimedia_project"));
            /**
             * Looks like a bug here. I can't use "views" to get the value of the field.
             */
            bqRow.set("views", row.getInt32("EXPR$1"));

            c.output(bqRow);
        }
    }
}
