/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import ch.qos.logback.core.FileAppender;

/**
 * Establishes test-specific logging without having to alter the global
 * <tt>logback-test.xml</tt> file. Allows directing output to the console
 * (if not already configured) and setting the log level on specific loggers
 * of interest in the test. The fixture automatically restores the original
 * log configuration on exit.
 * <p>
 * Typical usage: <pre><code>
 * {@literal @}Test
 * public void myTest() {
 *   LogFixtureBuilder logBuilder = LogFixture.builder()
 *          .toConsole()
 *          .disable() // Silence all other loggers
 *          .logger(ExternalSortBatch.class, Level.DEBUG);
 *   try (LogFixture logs = logBuilder.build()) {
 *     // Test code here
 *   }
 * }</code></pre>
 *  <p>
 * You can &ndash; and should &ndash; combine the log fixture with the
 * cluster and client fixtures to have complete control over your test-time
 * Drill environment.
 */

public class LogFixture implements AutoCloseable {

  // Elapsed time in ms, log level, thread, logger, message.

  public static final String DEFAULT_CONSOLE_FORMAT = "%r %level [%thread] [%logger] - %msg%n";
  private static final String DRILL_PACKAGE_NAME = "org.apache.drill";

  /**
   * Memento for a logger name and level.
   */
  public static class LogSpec {
    String loggerName;
    Level logLevel;

    public LogSpec(String loggerName, Level level) {
      this.loggerName = loggerName;
      this.logLevel = level;
    }
  }

  /**
   * Builds the log settings to be used for a test. The log settings here
   * add to those specified in a <tt>logback.xml</tt> or
   * <tt>logback-test.xml</tt> file on your class path. In particular, if
   * the logging configuration already redirects the Drill logger to the
   * console, setting console logging here does nothing.
   */

  public static class LogFixtureBuilder {

    private String consoleFormat = DEFAULT_CONSOLE_FORMAT;
    private boolean logToConsole;
    private File logToFile;
    private List<LogSpec> loggers = new ArrayList<>();

    /**
     * Send all enabled logging to the console (if not already configured.) Some
     * Drill log configuration files send the root to the console (or file), but
     * the Drill loggers to Lilith. In that case, Lilith "hides" the console
     * logger. Using this call adds a console logger to the Drill logger so that
     * output does, in fact, go to the console regardless of the configuration
     * in the Logback configuration file.
     *
     * @return this builder
     */
    public LogFixtureBuilder toConsole() {
      logToConsole = true;
      return this;
    }

    public LogFixtureBuilder toFile(File file) {
      logToFile = file;
      return this;
    }

    /**
     * Send logging to the console using the defined format.
     *
     * @param format valid Logback log format
     * @return this builder
     */

    public LogFixtureBuilder toConsole(String format) {
      consoleFormat = format;
      return toConsole();
    }

    /**
     * Set a specific logger to the given level.
     *
     * @param loggerName name of the logger (typically used for package-level
     * loggers)
     * @param level the desired Logback-defined level
     * @return this builder
     */
    public LogFixtureBuilder logger(String loggerName, Level level) {
      loggers.add(new LogSpec(loggerName, level));
      return this;
    }

    /**
     * Set a specific logger to the given level.
     *
     * @param loggerClass class that defines the logger (typically used for
     * class-specific loggers)
     * @param level the desired Logback-defined level
     * @return this builder
     */
    public LogFixtureBuilder logger(Class<?> loggerClass, Level level) {
      loggers.add(new LogSpec(loggerClass.getName(), level));
      return this;
    }

    /**
     * Turns off all logging. If called first, you can set disable as your
     * general policy, then turn back on loggers selectively for those
     * of interest.
     * @return this builder
     */
    public LogFixtureBuilder disable() {
      return rootLogger(Level.OFF);
    }

    /**
     * Set the desired log level on the root logger.
     * @param level the desired Logback log level
     * @return this builder
     */

    public LogFixtureBuilder rootLogger(Level level) {
      loggers.add(new LogSpec(Logger.ROOT_LOGGER_NAME, level));
      return this;
    }

    /**
     * Apply the log levels and output, then return a fixture to be used
     * in a try-with-resources block. The fixture automatically restores
     * the original configuration on completion of the try block.
     * @return the log fixture
     */
    public LogFixture build() {
      return new LogFixture(this);
    }
  }

  private PatternLayoutEncoder consolePle;
  private ConsoleAppender<ILoggingEvent> consoleAppender;
  private List<LogSpec> loggers = new ArrayList<>();
  private Logger drillLogger;
  private PatternLayoutEncoder filePle;
  private FileAppender<ILoggingEvent> fileAppender;
  private Logger root;
  private boolean rootConsoleSet;
  private boolean drillConsoleSet;

  public LogFixture(LogFixtureBuilder builder) {
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    drillLogger = (Logger)LoggerFactory.getLogger(DRILL_PACKAGE_NAME);
    if (builder.logToConsole) {
      setupConsole(builder);
    }
    if (builder.logToFile != null) {
      setupFile(builder);
    }
    setupLoggers(builder);
  }

  /**
   * Creates a new log fixture builder.
   * @return the log fixture builder
   */

  public static LogFixtureBuilder builder() {
    return new LogFixtureBuilder();
  }

  private void setupConsole(LogFixtureBuilder builder) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    consolePle = new PatternLayoutEncoder();
    consolePle.setPattern(builder.consoleFormat);
    consolePle.setContext(lc);
    consolePle.start();

    consoleAppender = new ConsoleAppender<ILoggingEvent>( );
    consoleAppender.setContext(lc);
    consoleAppender.setName("Console");
    consoleAppender.setEncoder(consolePle);
    consoleAppender.start();

    if (root.getAppender("STDOUT") == null) {
      root.addAppender(consoleAppender);
      rootConsoleSet = true;
    }
    if (drillLogger.getAppender("STDOUT") == null) {
      drillLogger.addAppender(consoleAppender);
      drillConsoleSet = true;
    }
  }

  private void setupFile(LogFixtureBuilder builder) {
    LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
    filePle = new PatternLayoutEncoder();
    filePle.setPattern("%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n");
    filePle.setContext(lc);
    filePle.start();

    fileAppender = new FileAppender<ILoggingEvent>( );
    fileAppender.setContext(lc);
    fileAppender.setFile(builder.logToFile.getAbsolutePath());
    fileAppender.setName("Custom File");
    fileAppender.start();
    fileAppender.setEncoder(filePle);
    root.addAppender(fileAppender);
    drillLogger.addAppender(fileAppender);
  }

  private void setupLoggers(LogFixtureBuilder builder) {
    for (LogSpec spec : builder.loggers) {
      setupLogger(spec);
    }
  }

  private void setupLogger(LogSpec spec) {
    Logger logger = (Logger)LoggerFactory.getLogger(spec.loggerName);
    Level oldLevel = logger.getLevel();
    logger.setLevel(spec.logLevel);
    loggers.add(new LogSpec(spec.loggerName, oldLevel));
  }

  @Override
  public void close() {
    restoreLoggers();
    restoreConsole();
    restoreFile();
  }

  private void restoreLoggers() {
    for (LogSpec spec : loggers) {
      Logger logger = (Logger)LoggerFactory.getLogger(spec.loggerName);
      logger.setLevel(spec.logLevel);
    }
  }

  private void restoreConsole() {
    if (consoleAppender == null) {
      return;
    }
    if (rootConsoleSet) {
      root.detachAppender(consoleAppender);
    }
    if (drillConsoleSet) {
      drillLogger.detachAppender(consoleAppender);
    }
    consoleAppender.stop();
    consolePle.stop();
  }

  private void restoreFile() {
    if (fileAppender == null) {
      return;
    }
    root.detachAppender(fileAppender);
    drillLogger.detachAppender(fileAppender);
    fileAppender.stop();
  }
}
