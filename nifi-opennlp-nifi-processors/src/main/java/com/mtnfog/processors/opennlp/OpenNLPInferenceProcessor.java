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

import com.google.gson.Gson;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.util.Span;
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
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"opennlp"})
@CapabilityDescription("Performs inference using an Apache OpenNLP model")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class OpenNLPInferenceProcessor extends AbstractProcessor {

    public static final PropertyDescriptor MODEL_FILE_NAME = new PropertyDescriptor
            .Builder().name("MODEL_FILE_NAME")
            .displayName("Model file name")
            .description("The file name of the trained model")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship INFERENCE_COMPLETE = new Relationship.Builder()
            .name("INFERENCE_COMPLETE")
            .description("Model successfully applied")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Failure while performing inference")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final Gson gson = new Gson();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(MODEL_FILE_NAME);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(INFERENCE_COMPLETE);
        relationships.add(FAILURE);
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

        getLogger().info("Inference processor triggered");

        try {

            final InputStream is = new FileInputStream("en-ner-person.bin");

            final TokenNameFinderModel model = new TokenNameFinderModel(is);
            is.close();

            final NameFinderME nameFinder = new NameFinderME(model);

            flowFile = session.write(flowFile, (in, out) -> {

                try (BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()));
                     BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, Charset.defaultCharset()));) {

                    String line;
                    while (null != (line = br.readLine())) {

                        final String[] tokens = line.split(" ");
                        final Span[] spans = nameFinder.find(tokens);

                        final String updatedValue = gson.toJson(spans);
                        bw.write(updatedValue);

                    }

                }
            });

            session.transfer(flowFile, INFERENCE_COMPLETE);

        } catch (Exception ex) {
            getLogger().error("Unable to perform inference.", ex);
            session.transfer(flowFile, FAILURE);
        }

    }

}
