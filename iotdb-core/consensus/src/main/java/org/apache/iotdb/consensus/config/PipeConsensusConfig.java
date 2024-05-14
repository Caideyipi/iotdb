package org.apache.iotdb.consensus.config;

import org.apache.iotdb.commons.pipe.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeDispatcher;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeGuardian;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeSelector;
import org.apache.iotdb.consensus.pipe.consensuspipe.ProgressIndexManager;

public class PipeConsensusConfig {
  private final Pipe pipe;

  public PipeConsensusConfig(Pipe pipe) {
    this.pipe = pipe;
  }

  public Pipe getPipe() {
    return pipe;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private Pipe pipe;

    public Builder setPipe(Pipe pipe) {
      this.pipe = pipe;
      return this;
    }

    public PipeConsensusConfig build() {
      return new PipeConsensusConfig(pipe);
    }
  }

  public static class Pipe {
    private final String extractorPluginName;
    private final String processorPluginName;
    private final String connectorPluginName;
    private final ConsensusPipeDispatcher consensusPipeDispatcher;
    private final ConsensusPipeGuardian consensusPipeGuardian;
    private final ConsensusPipeSelector consensusPipeSelector;
    private final ProgressIndexManager progressIndexManager;
    private final long consensusPipeGuardJobIntervalInSeconds;

    public Pipe(
        String extractorPluginName,
        String processorPluginName,
        String connectorPluginName,
        ConsensusPipeDispatcher consensusPipeDispatcher,
        ConsensusPipeGuardian consensusPipeGuardian,
        ConsensusPipeSelector consensusPipeSelector,
        ProgressIndexManager progressIndexManager,
        long consensusPipeGuardJobIntervalInSeconds) {
      this.extractorPluginName = extractorPluginName;
      this.processorPluginName = processorPluginName;
      this.connectorPluginName = connectorPluginName;
      this.consensusPipeDispatcher = consensusPipeDispatcher;
      this.consensusPipeGuardian = consensusPipeGuardian;
      this.consensusPipeSelector = consensusPipeSelector;
      this.progressIndexManager = progressIndexManager;
      this.consensusPipeGuardJobIntervalInSeconds = consensusPipeGuardJobIntervalInSeconds;
    }

    public String getExtractorPluginName() {
      return extractorPluginName;
    }

    public String getProcessorPluginName() {
      return processorPluginName;
    }

    public String getConnectorPluginName() {
      return connectorPluginName;
    }

    public ConsensusPipeDispatcher getConsensusPipeDispatcher() {
      return consensusPipeDispatcher;
    }

    public ConsensusPipeGuardian getConsensusPipeGuardian() {
      return consensusPipeGuardian;
    }

    public ConsensusPipeSelector getConsensusPipeSelector() {
      return consensusPipeSelector;
    }

    public ProgressIndexManager getProgressIndexManager() {
      return progressIndexManager;
    }

    public long getConsensusPipeGuardJobIntervalInSeconds() {
      return consensusPipeGuardJobIntervalInSeconds;
    }

    public static Pipe.Builder newBuilder() {
      return new Pipe.Builder();
    }

    public static class Builder {
      private String extractorPluginName = BuiltinPipePlugin.IOTDB_EXTRACTOR.getPipePluginName();
      private String processorPluginName =
          BuiltinPipePlugin.DO_NOTHING_PROCESSOR.getPipePluginName();
      private String connectorPluginName =
          BuiltinPipePlugin.DO_NOTHING_CONNECTOR.getPipePluginName();
      private ConsensusPipeDispatcher consensusPipeDispatcher = null;
      private ConsensusPipeGuardian consensusPipeGuardian = null;
      private ConsensusPipeSelector consensusPipeSelector = null;
      private ProgressIndexManager progressIndexManager = null;
      private long consensusPipeGuardJobIntervalInSeconds = 180L;

      public Pipe.Builder setExtractorPluginName(String extractorPluginName) {
        this.extractorPluginName = extractorPluginName;
        return this;
      }

      public Pipe.Builder setProcessorPluginName(String processorPluginName) {
        this.processorPluginName = processorPluginName;
        return this;
      }

      public Pipe.Builder setConnectorPluginName(String connectorPluginName) {
        this.connectorPluginName = connectorPluginName;
        return this;
      }

      public Pipe.Builder setConsensusPipeDispatcher(
          ConsensusPipeDispatcher consensusPipeDispatcher) {
        this.consensusPipeDispatcher = consensusPipeDispatcher;
        return this;
      }

      public Pipe.Builder setConsensusPipeGuardian(ConsensusPipeGuardian consensusPipeGuardian) {
        this.consensusPipeGuardian = consensusPipeGuardian;
        return this;
      }

      public Pipe.Builder setConsensusPipeSelector(ConsensusPipeSelector consensusPipeSelector) {
        this.consensusPipeSelector = consensusPipeSelector;
        return this;
      }

      public Pipe.Builder setProgressIndexManager(ProgressIndexManager progressIndexManager) {
        this.progressIndexManager = progressIndexManager;
        return this;
      }

      public Pipe.Builder setConsensusPipeGuardJobIntervalInSeconds(
          long consensusPipeGuardJobIntervalInSeconds) {
        this.consensusPipeGuardJobIntervalInSeconds = consensusPipeGuardJobIntervalInSeconds;
        return this;
      }

      public Pipe build() {
        return new Pipe(
            extractorPluginName,
            processorPluginName,
            connectorPluginName,
            consensusPipeDispatcher,
            consensusPipeGuardian,
            consensusPipeSelector,
            progressIndexManager,
            consensusPipeGuardJobIntervalInSeconds);
      }
    }
  }
}
