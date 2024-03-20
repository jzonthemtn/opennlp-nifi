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
package com.mtnfog.processors.opennlp;

import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.NameSample;
import opennlp.tools.namefind.NameSampleDataStream;
import opennlp.tools.namefind.TokenNameFinderEvaluator;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.InputStreamFactory;
import opennlp.tools.util.MarkableFileInputStreamFactory;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"opennlp"})
@CapabilityDescription("Evaluates an Apache OpenNLP model")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class OpenNLPModelEvaluateProcessor extends AbstractProcessor {

    public static final PropertyDescriptor EVALUATION_DATA = new PropertyDescriptor
            .Builder().name("EVALUATION_DATA")
            .displayName("Evaluation data")
            .description("Local file containing the evaluation data")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor THRESHOLD_PRECISION = new PropertyDescriptor
            .Builder().name("THRESHOLD_PRECISION")
            .displayName("Precision threshold")
            .description("The precision threshold for deploying the model")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor THRESHOLD_RECALL = new PropertyDescriptor
            .Builder().name("THRESHOLD_RECALL")
            .displayName("Recall threshold")
            .description("The recall threshold for deploying the model")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship VALIDATION_SUCCESSFUL = new Relationship.Builder()
            .name("VALIDATION_SUCCESSFUL")
            .description("The validation was successful")
            .build();

    public static final Relationship VALIDATION_FAILED = new Relationship.Builder()
            .name("VALIDATION_FAILED")
            .description("Validation did not meet the required thresholds")
            .build();

    public static final Relationship UNABLE_TO_VALIDATE = new Relationship.Builder()
            .name("UNABLE_TO_VALIDATE")
            .description("Unable to validate the model")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(THRESHOLD_PRECISION);
        descriptors.add(THRESHOLD_RECALL);
        descriptors.add(EVALUATION_DATA);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(VALIDATION_SUCCESSFUL);
        relationships.add(VALIDATION_FAILED);
        relationships.add(UNABLE_TO_VALIDATE);
        relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {

            final String testFile = context.getProperty(EVALUATION_DATA).getValue();
            final String modelFile = flowFile.getAttribute("model_file");

            try (final InputStream modelIn = new FileInputStream(modelFile)) {

                final TokenNameFinderModel model = new TokenNameFinderModel(modelIn);

                final InputStreamFactory in = new MarkableFileInputStreamFactory(new File(testFile));
                final ObjectStream<NameSample> sampleStream = new NameSampleDataStream(new PlainTextByLineStream(in, StandardCharsets.UTF_8));
                final TokenNameFinderEvaluator evaluator = new TokenNameFinderEvaluator(new NameFinderME(model));
                evaluator.evaluate(sampleStream);

                final double precisionThreshold = context.getProperty(THRESHOLD_PRECISION).asDouble();
                final double precision = evaluator.getFMeasure().getPrecisionScore();

                final double recallThreshold = context.getProperty(THRESHOLD_RECALL).asDouble();
                final double recall = evaluator.getFMeasure().getRecallScore();

                if(precision >= precisionThreshold && recall >= recallThreshold) {
                    getLogger().info("Validation successful with precision " + precision + " >= " + precisionThreshold);
                    getLogger().info("Validation successful with recall " + recall + " >= " + recallThreshold);
                    session.transfer(flowFile, VALIDATION_SUCCESSFUL);
                } else {
                    getLogger().info("Validation failed with precision " + precision + " < " + precisionThreshold);
                    getLogger().info("Validation failed with precision " + recall + " < " + recallThreshold);
                    session.transfer(flowFile, VALIDATION_FAILED);
                }

            }

        } catch (Exception ex) {
            getLogger().error("Unable to validate model.", ex);
            session.transfer(flowFile, UNABLE_TO_VALIDATE);
        }

    }

}
