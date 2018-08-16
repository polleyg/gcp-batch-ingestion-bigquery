package org.polleyg;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNot.not;


public class TemplatePipelineTest {

    private static DoFnTester<String, TableRow> fnTester;

    @BeforeClass
    public static void init() {
        fnTester = DoFnTester.of(new TemplatePipeline.WikiParDo());
    }

    @AfterClass
    public static void destroy() {
        fnTester = null;
    }

    @Test
    public void test_parse_CSV_format_successfully_with_tablerow() throws Exception {

        List<String> input = new ArrayList<>();

        input.add("2018,8,13,Wikinews,English,Spanish football: Sevilla signs Aleix Vidal from FC Barcelona,12331");

        List<TableRow> output = fnTester.processBundle(input);

        Assert.assertThat(output, is(not(empty())));

        Assert.assertThat(output.get(0).get("year"), is(equalTo("2018")));
        Assert.assertThat(output.get(0).get("month"), is(equalTo("8")));
        Assert.assertThat(output.get(0).get("day"), is(equalTo("13")));
        Assert.assertThat(output.get(0).get("wikimedia_project"), is(equalTo("Wikinews")));
        Assert.assertThat(output.get(0).get("language"), is(equalTo("English")));
        Assert.assertThat(output.get(0).get("title"), is(equalTo("Spanish football: Sevilla signs Aleix Vidal from FC Barcelona")));
        Assert.assertThat(output.get(0).get("views"), is(equalTo("12331")));
    }

    @Test
    public void test_parse_header_return_empty_list() throws Exception {

        List<String> input = new ArrayList<>();

        input.add("year,month,day,wikimedia_project,language,title,views");

        List<TableRow> output = fnTester.processBundle(input);

        Assert.assertThat(output, is(empty()));
    }
}
