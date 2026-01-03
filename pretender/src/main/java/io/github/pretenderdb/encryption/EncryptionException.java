package io.github.pretenderdb.encryption;

/**
 * Exception thrown when encryption or decryption operations fail.
 */
public class EncryptionException extends RuntimeException {

  /**
   * Instantiates a new Encryption exception.
   *
   * @param message the message
   */
  public EncryptionException(final String message) {
    super(message);
  }

  /**
   * Instantiates a new Encryption exception.
   *
   * @param message the message
   * @param cause   the cause
   */
  public EncryptionException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
