package introdb.heap;

public final class TooManyPages extends RuntimeException {

  TooManyPages(final long maxNrPages) {
    super("The limit on the number of pages is " + maxNrPages + ". It cannot be exceeded.");
  }

}
