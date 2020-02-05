package org.apache.drill.exec.store.easy.json.structparser;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.easy.json.parser.JsonLoaderImpl;
import org.apache.drill.exec.store.easy.json.parser.NullTypeMarker;
import org.apache.drill.exec.store.easy.json.parser.RecoverableJsonException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonStructureParser {
  protected static final Logger logger = LoggerFactory.getLogger(JsonStructureParser.class);

  public static final String ROOT_NAME = "<root>";

  final JsonParser parser;
  private final JsonOptions options;
  private final ErrorFactory errorFactory;
  private final TokenIterator tokenizer;
  private final JsonElementParser rootState;
  private int errorRecoveryCount;

  // Using a simple list. Won't perform well if we have hundreds of
  // null fields; but then we've never seen such a pathologically bad
  // case... Usually just one or two fields have deferred nulls.

  private final List<NullTypeMarker> nullStates = new ArrayList<>();

  public JsonStructureParser(InputStream stream, JsonOptions options,
      ObjectListener rootListener, ErrorFactory errorFactory) {
    this.errorFactory = errorFactory;
    try {
      ObjectMapper mapper = new ObjectMapper()
          .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
          .configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      if (options.allowNanInf) {
        mapper.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
      }

      parser = mapper.getFactory().createParser(stream);
    } catch (JsonParseException e) {
      throw errorFactory().parseError("Failed to create the JSON parser", e);
    } catch (IOException e) {
      throw errorFactory().ioException("Failed to open the JSON parser", e);
    }
    this.options = options;
    tokenizer = new TokenIterator(parser, options, errorFactory);
    rootState = makeRootState(rootListener);
  }

  public ErrorFactory errorFactory() { return errorFactory; }

  public JsonOptions options() { return options; }

  private JsonElementParser makeRootState(ObjectListener rootListener) {
    JsonToken token = tokenizer.next();
    if (token == null) {
      return null;
    }
    switch (token) {

      // File contains an array of records.

      case START_ARRAY:
        if (options.skipOuterList) {
          return new RootArrayParser(this, rootListener);
        } else {
          throw errorFactory.structureError(
              "JSON includes an outer array, but outer array support is not enabled");
        }

      // File contains a sequence of one or more records,
      // presumably sequentially.

      case START_OBJECT:
        tokenizer.unget(token);
        return new RootTupleState(this, rootListener);

      // Not a valid JSON file for Drill.

      default:
        throw errorFactory.syntaxError(token);
    }
  }

  public void addNullMarker(NullTypeMarker marker) {
    nullStates.add(marker);
  }

  public void removeNullMarker(NullTypeMarker marker) {
    nullStates.remove(marker);
  }

  /**
   * Finish reading a batch of data. We may have pending "null" columns:
   * a column for which we've seen only nulls, or an array that has
   * always been empty. The batch needs to finish, and needs a type,
   * but we still don't know the type. Since we must decide on one,
   * we do the following:
   * <ul>
   * <li>If given a type negotiator, ask it the type. Perhaps a
   * prior reader in the same scanner determined the type in a
   * previous file.</li>
   * <li>Guess {@code VARCHAR}, and switch to text mode.</li>
   * </ul>
   *
   * Note that neither of these choices is perfect. There is no guarantee
   * that prior files either were seen, or have the same type as this
   * file. Also, switching to text mode means results will vary
   * from run to run depending on the order that we see empty and
   * non-empty values for this column. Plus, since the system is
   * distributed, the decision made here may conflict with that made in
   * some other fragment.
   * <p>
   * The only real solution is for the user to provide schema
   * information to resolve the ambiguity; but Drill has no way to
   * gather that information at present.
   * <p>
   * Bottom line: the user is responsible for not giving Drill
   * ambiguous data that would require Drill to predict the future.
   */
  public void forceNullResolution() {
    List<NullTypeMarker> copy = new ArrayList<>();
    copy.addAll(nullStates);
    for (NullTypeMarker marker : copy) {
      marker.forceResolution();
    }
    assert nullStates.isEmpty();
  }

  /**
   * Attempt recovery from a JSON syntax error by skipping to the next
   * record. The Jackson parser is quite limited in its recovery abilities.
   *
   * @return {@code true{@code  if another record can be read, {@code false}
   * if EOF.
   * @throws UserException if the error is unrecoverable
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-4653">DRILL-4653</a>
   * @see <a href="https://issues.apache.org/jira/browse/DRILL-5953">DRILL-5953</a>
   */
  private boolean recover() {
    logger.warn("Attempting recovery from JSON syntax error. " + tokenizer.context());
    boolean firstAttempt = true;
    for (;;) {
      for (;;) {
        try {
          if (parser.isClosed()) {
            throw errorFactory().unrecoverableError();
          }
          JsonToken token = tokenizer.next();
          if (token == null) {
            if (firstAttempt) {
              throw errorFactory().unrecoverableError();
            }
            return false;
          }
          if (token == JsonToken.NOT_AVAILABLE) {
            return false;
          }
          if (token == JsonToken.END_OBJECT) {
            break;
          }
          firstAttempt = false;
        } catch (RecoverableJsonException e) {
          // Ignore, keep trying
        }
      }
      try {
        JsonToken token = tokenizer.next();
        if (token == null || token == JsonToken.NOT_AVAILABLE) {
          return false;
        }
        if (token == JsonToken.START_OBJECT) {
          logger.warn("Attempting to resume JSON parse. " + tokenizer.context());
          tokenizer.unget(token);
          errorRecoveryCount++;
          return true;
        }
      } catch (RecoverableJsonException e) {
        // Ignore, keep trying
      }
    }
  }

  public int recoverableErrorCount() { return errorRecoveryCount; }

  public void close() {
    if (errorRecoveryCount > 0) {
      logger.warn("Read JSON input with {} recoverable error(s).",
          errorRecoveryCount);
    }
    try {
      parser.close();
    } catch (IOException e) {
      logger.warn("Ignored failure when closing JSON source", e);
    }
  }
}
