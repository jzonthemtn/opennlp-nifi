# Apache OpenNLP Processors for Apache NiFi

This repository contains [Apache NiFi](https://nifi.apache.org) processors that use [Apache OpenNLP](https://opennlp.apache.org) for NLP model training, evaluation, and deployment.

With these processors you can retrieve NLP model training data, convert it to the OpenNLP training format, train a model, evaluate the model, and optionally deploy a model should the trained model meet the thresholds necessary for deployment.

To use the processors:

```
mvn clean package
```

Copy the built NAR file to your Apache NiFi `lib/` directory:

```
cp ./nifi-opennlp-nifi-nar/target/nifi-opennlp-nifi-nar-0.0.1.nar /path/to/nifi/lib
```

Now restart NiFi. The processors will now be available for use in your NiFi flows.
