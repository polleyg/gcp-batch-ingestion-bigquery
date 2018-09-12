package org.polleyg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyManager {

    private static Logger logger = LoggerFactory.getLogger(PropertyManager.class);
    private static PropertyManager pm = null;
    public static final String[] arguments = {

//            "--project=karthikeysurineni-215500",
            "--project=gcp-batch-pattern",
                    "--runner=DirectRunner",
            "--gcpTempLocation=gs://karthikey-surineni-beam-sql/temp",
            "--stagingLocation=gs://karthikey-surineni-beam-sql/staging"
    };

    public static final String[] resources = {

//            "GCS_FILE", "gs://karthikey-surineni-beam-sql/Wiki1k.csv"
            "GCS_FILE", "gs://dataflow-beam-sql-pipeline-wiki1k/wiki1k.csv"


    };

    private PropertyManager(){

    }

    public String getProperty(String res){

        for(int i=0;i<resources.length;i=i+2)
            if(res.equalsIgnoreCase(resources[i]))
                return resources[i+1];

            logger.warn("Resource "+res+" not found!");
            return null;
    }

    public static PropertyManager getInstance(){

        if(pm == null)
            pm = new PropertyManager();
        return pm;
    }

}

