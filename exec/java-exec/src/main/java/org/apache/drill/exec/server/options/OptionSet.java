package org.apache.drill.exec.server.options;

/**
 * Immutable set of options accessible by name or validator.
 */

public interface OptionSet {

  /**
   * Gets the option value for the given option name.
   *
   * This interface also provides convenient methods to get typed option values:
   * {@link #getOption(TypeValidators.BooleanValidator validator)},
   * {@link #getOption(TypeValidators.DoubleValidator validator)},
   * {@link #getOption(TypeValidators.LongValidator validator)}, and
   * {@link #getOption(TypeValidators.StringValidator validator)}.
   *
   * @param name option name
   * @return the option value, null if the option does not exist
   */
  OptionValue getOption(String name);

  /**
   * Gets the boolean value (from the option value) for the given boolean validator.
   *
   * @param validator the boolean validator
   * @return the boolean value
   */
  boolean getOption(TypeValidators.BooleanValidator validator);

  /**
   * Gets the double value (from the option value) for the given double validator.
   *
   * @param validator the double validator
   * @return the double value
   */
  double getOption(TypeValidators.DoubleValidator validator);

  /**
   * Gets the long value (from the option value) for the given long validator.
   *
   * @param validator the long validator
   * @return the long value
   */
  long getOption(TypeValidators.LongValidator validator);

  /**
   * Gets the string value (from the option value) for the given string validator.
   *
   * @param validator the string validator
   * @return the string value
   */
  String getOption(TypeValidators.StringValidator validator);

}
