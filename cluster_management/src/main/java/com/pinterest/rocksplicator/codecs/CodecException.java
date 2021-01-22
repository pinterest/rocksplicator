package com.pinterest.rocksplicator.codecs;

public class CodecException extends Exception {

  public CodecException() {
  }

  public CodecException(String message) {
    super(message);
  }

  public CodecException(String message, Throwable cause) {
    super(message, cause);
  }

  public CodecException(Throwable cause) {
    super(cause);
  }

  public CodecException(String message, Throwable cause, boolean enableSuppression,
                        boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
