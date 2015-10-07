/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jeremydyer.nifi.openalpr;

import com.openalpr.jni.Alpr;
import com.openalpr.jni.AlprPlate;
import com.openalpr.jni.AlprPlateResult;
import com.openalpr.jni.AlprResults;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Tags({"openalpr"})
@CapabilityDescription("Uses OpenALPR to recognize license plates")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
public class OpenALPRProcessor extends AbstractProcessor {

    public static final PropertyDescriptor PRO_COUNTRY_CODE = new PropertyDescriptor
            .Builder().name("OpenALPR Country Code")
            .description("Country code used to locate the license plate. Ex. usa, us, eu")
            .defaultValue("us")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRO_OPENALPR_CONFIG_PATH = new PropertyDescriptor
            .Builder().name("OpenALPR Configuration File Path")
            .description("Location of the OpenALPR configuration file")
            .defaultValue("/etc/openalpr/openalpr.conf")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRO_OPENALPR_RUNTIME_PATH = new PropertyDescriptor
            .Builder().name("OpenALPR Runtime File Path")
            .description("Location of the OpenALPR runtime data. This includes the models for locating images and OCR configuration files.")
            .defaultValue("/usr/share/openalpr/runtime_data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRO_OPENALPR_TOP_NUM_RESULTS = new PropertyDescriptor
            .Builder().name("OpenALPR topN")
            .description("Number of top results to return")
            .defaultValue("10")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRO_OPENALPR_DEFAULT_REGION = new PropertyDescriptor
            .Builder().name("OpenALPR Default Region")
            .description("Default region for detection. This is only supported in the 'us' country code and is experimental")
            .defaultValue("ga")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRO_INPUT_IMAGE = new PropertyDescriptor
            .Builder().name("License Plate Image")
            .description("Read a single license plate image from this file for testing.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("my_relationship")
            .description("Example relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("problem during execution")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PRO_COUNTRY_CODE);
        descriptors.add(PRO_OPENALPR_CONFIG_PATH);
        descriptors.add(PRO_OPENALPR_RUNTIME_PATH);

        descriptors.add(PRO_OPENALPR_TOP_NUM_RESULTS);
        descriptors.add(PRO_OPENALPR_DEFAULT_REGION);
        descriptors.add(PRO_INPUT_IMAGE);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("Scheduled task has been called");
        try {
            getLogger().info("Creating Alpr object instance!");

            Alpr alpr = new Alpr(context.getProperty(PRO_COUNTRY_CODE).getValue(),
                    context.getProperty(PRO_OPENALPR_CONFIG_PATH).getValue(),
                    context.getProperty(PRO_OPENALPR_RUNTIME_PATH).getValue());
            alpr.setTopN(context.getProperty(PRO_OPENALPR_TOP_NUM_RESULTS).asInteger());
            alpr.setDefaultRegion("wa");

            //Read the image data from the flowfile to begin the analysis.
            //TODO: right now for testing just reading a standard image file
            Path path = Paths.get(context.getProperty(PRO_INPUT_IMAGE).getValue());
            byte[] imagedata = Files.readAllBytes(path);

            AlprResults results = alpr.recognize(imagedata);

            for (AlprPlateResult result : results.getPlates()) {
                for (AlprPlate plate : result.getTopNPlates()) {
                    getLogger().info("Found License Plate: '" + plate.getCharacters() + "' wich confidence of: " + plate.getOverallConfidence() + "% in " + results.getTotalProcessingTimeMs() + " ms");
                }
            }

            //Make sure to release all of the memory that was being held
            alpr.unload();

        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Error in OpenALPR: ", ex);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }

        try {
            getLogger().info("Creating Alpr object instance!");

            Alpr alpr = new Alpr(context.getProperty(PRO_COUNTRY_CODE).getValue(),
                    context.getProperty(PRO_OPENALPR_CONFIG_PATH).getValue(),
                    context.getProperty(PRO_OPENALPR_RUNTIME_PATH).getValue());
            alpr.setTopN(context.getProperty(PRO_OPENALPR_TOP_NUM_RESULTS).asInteger());
            alpr.setDefaultRegion("wa");

            //Read the image data from the flowfile to begin the analysis.
            //TODO: right now for testing just reading a standard image file
            Path path = Paths.get(context.getProperty(PRO_INPUT_IMAGE).getValue());
            byte[] imagedata = Files.readAllBytes(path);

            AlprResults results = alpr.recognize(imagedata);

            for (AlprPlateResult result : results.getPlates()) {
                for (AlprPlate plate : result.getTopNPlates()) {
                    getLogger().info("Found License Plate: '" + plate.getCharacters() + "' wich confidence of: " + plate.getOverallConfidence() + "% in " + results.getTotalProcessingTimeMs() + " ms");
                }
            }

            session.transfer(flowFile, MY_RELATIONSHIP);

            //Make sure to release all of the memory that was being held
            alpr.unload();

        } catch (Exception ex) {
            ex.printStackTrace();
            getLogger().error("Error in OpenALPR: ", ex);
            session.transfer(flowFile, REL_FAILURE);
        }

    }

}
