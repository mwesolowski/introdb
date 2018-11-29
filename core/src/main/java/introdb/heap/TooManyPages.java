package introdb.heap;

public class TooManyPages extends RuntimeException {

  TooManyPages(final long maxNrPages) {
    super("The limit of the number of pages is " + maxNrPages + ". It cannot be exceeded.");
  }

}
