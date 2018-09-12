package org.polleyg;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Do some randomness
 */
public class TemplatePipeline {


    protected static Logger logger = LoggerFactory.getLogger(TemplatePipeline.class);
    protected final static Schema tableSchema = createWikiSchema();


//    protected final static String BIGQUERY_TABLE = "karthikeysurineni-215500:dotc_2018.wiki_demo";
//    protected final static String GCP_TEMP_LOC = "gs://karthikey-surineni-beam-sql";

    public static void main(String[] args) {
        PipelineOptionsFactory.register(DataflowPipelineOptions.class);
        args=(args==null?PropertyManager.getInstance().arguments:args);

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
//        options.setTempLocation("gs://karthikey-surineni-beam-sql/temp");

        Pipeline pipeline = Pipeline.create(options);
//        pipeline.apply("READ", TextIO.read().from(GCS_FILE))
//                .apply("TRANSFORM", ParDo.of(new WikiParDo()))
//                .apply("WRITE", BigQueryIO.writeTableRows()
//                        .to(BIGQUERY_TABLE)
//                        .withCreateDisposition(CREATE_IF_NEEDED)
//                        .withWriteDisposition(WRITE_APPEND)
//                        .withSchema(getTableSchema()));


      //Create a Collection of records from the Wiki1k csv file
      PCollection<Row> inputTable = PBegin.in(pipeline)
              .apply("READ", TextIO.read().from(PropertyManager.getInstance().getProperty("GCS_FILE")))
              .apply(ParDo.of(new WikiBeamParDo()))
              .setCoder(tableSchema.getRowCoder());



      //CASE 1: Collection to Count wikimedia projects
      PCollection<Row> o1Stream = inputTable
              .apply(SqlTransform.query("select `wikimedia_project`,sum(1) from PCOLLECTION group by `wikimedia_project`"
                                        ));
      o1Stream.apply("CASE1_RESULT",MapElements.via(new SimpleFunction<Row, Void>() {
          @Override
          public @Nullable
          Void apply(Row input) {
              logger.info(input.getValues().toString());
              return null;
          }
      }));


        pipeline.run().waitUntilFinish();

    }

//    private static TableSchema getTableSchema() {
//        List<TableFieldSchema> fields = new ArrayList<>();
//        fields.add(new TableFieldSchema().setName("year").setType("INTEGER"));
//        fields.add(new TableFieldSchema().setName("month").setType("INTEGER"));
//        fields.add(new TableFieldSchema().setName("day").setType("INTEGER"));
//        fields.add(new TableFieldSchema().setName("wikimedia_project").setType("STRING"));
//        fields.add(new TableFieldSchema().setName("language").setType("STRING"));
//        fields.add(new TableFieldSchema().setName("title").setType("STRING"));
//        fields.add(new TableFieldSchema().setName("views").setType("INTEGER"));
//        return new TableSchema().setFields(fields);
//    }
//
//    public interface TemplateOptions extends DataflowPipelineOptions {
//        @Description("GCS path of the file to read from")
//        @Default.String("gs://karthikey-surineni-beam-sql/Wiki1k.csv")
//        ValueProvider<String> getInputFile();
//
//        void setInputFile(ValueProvider<String> value);

//    }


    //Creates the schema format for the Wiki records
    private static Schema createWikiSchema() {
        // Define the schema for the records.
        Schema wikiMediaSchema =
                Schema
                        .builder()
                        .addInt32Field("year")
                        .addInt32Field("month")
                        .addInt32Field("day")
                        .addStringField("wikimedia_project")
                        .addStringField("langauge")
                        .addStringField("title")
                        .addInt32Field("views")
                        .build();

        return wikiMediaSchema;
    }

    //Creates the row
    private static Row.Builder createRow(){
        Row.Builder rowBuilder =
                Row
                        .withSchema(tableSchema);
        return rowBuilder;
    }

//    public static class WikiParDo extends DoFn<String, TableRow> {
//        public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";
//
//        @ProcessElement
//        public void processElement(ProcessContext c) throws Exception {
//            if (c.element().equalsIgnoreCase(HEADER)) return;
//            String[] split = c.element().split(",");
//            if (split.length > 7) return;
//            TableRow row = new TableRow();
//            for (int i = 0; i < split.length; i++) {
//                TableFieldSchema col = getTableSchema().getFields().get(i);
//                row.set(col.getName(), split[i]);
//            }
//            c.output(row);
//        }

        public static class WikiBeamParDo extends DoFn<String, Row> {
            public static final String HEADER = "year,month,day,wikimedia_project,language,title,views";


            @ProcessElement
            public void processElement(ProcessContext c) throws Exception {

                Row.Builder rowBuilder = createRow();
                if (c.element().equalsIgnoreCase(HEADER)) return;
                String[] split = c.element().split(",");
                if (split.length > 7) return;
                for (int i = 0; i < split.length; i++) {

                    // Create a concrete row with that type.
                    if(i==0||i==1||i==2||i==6)
                        rowBuilder.addValue(Integer.parseInt(split[i]));
                    else
                    rowBuilder.addValue(split[i]);

                }

                Row row = rowBuilder.build();
                c.output(row);
                Create.of(row);
            }


        }


    }
