package org.apache.drill.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;

import com.google.common.io.Closeables;

public class FileMatcher {

  public interface Filter {
    boolean accept( String line );
  }

  public static interface Source {
    Reader reader( ) throws IOException;
  }

  public static class FileSource implements Source {
    File file;

    public FileSource( File file ) {
      this.file = file;
    }

    @Override
    public Reader reader( ) throws IOException {
      return new FileReader( file );
    }
  }

  public static class StringSource implements Source {

    String string;

    public StringSource( String string ) {
      this.string = string;
    }

    @Override
    public Reader reader( ) {
      return new StringReader( string );
    }
  }

  public static class ReaderSource implements Source {

    Reader reader;

    public ReaderSource( Reader reader ) {
      this.reader = reader;
    }

    @Override
    public Reader reader( ) {
      return reader;
    }
  }

  public static class ResourceSource implements Source {

    String resource;

    public ResourceSource( String resource ) {
      this.resource = resource;
    }

    @Override
    public Reader reader( ) {
      try {
        return new InputStreamReader( getClass( ).getResourceAsStream( resource ), "UTF-8" );
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException( e );
      }
    }
  }

  public static class Builder {
    private boolean ignoreWhitespace;
    private Filter filter;
    private Source expected;
    private Source actual;

    Builder expectedFile( File file ) {
      expected = new FileSource( file );
      return this;
   }

    Builder expectedString( String text ) {
      expected = new StringSource( text );
      return this;
    }

    Builder expectedReader( Reader in ) {
      expected = new ReaderSource( in );
      return this;
    }

    Builder expectedResource( String resource ) {
      expected = new ResourceSource( resource );
      return this;
    }

    Builder actualFile( File file ) {
      actual = new FileSource( file );
      return this;
    }

    Builder actualString( String text ) {
      actual = new StringSource( text );
      return this;
    }

    Builder actualReader( Reader in ) {
      actual = new ReaderSource( in );
      return this;
    }

    Builder actualResource( String resource ) {
      actual = new ResourceSource( resource );
      return this;
    }

    Builder ignoreWhitespace( boolean flag ) {
      ignoreWhitespace = flag;
      return this;
    }

    Builder withFilter( Filter filter ) {
      this.filter = filter;
      return this;
    }
  }

  private Builder builder;
  private LineNumberReader expected;
  private LineNumberReader actual;

  private FileMatcher( Builder builder ) {
    this.builder = builder;
  }

  public boolean match( ) throws IOException {
    try {
      open( );
      return compare( );
    }
    finally {
      close( );
    }
  }

  private void open() throws IOException {
    expected = new LineNumberReader( builder.expected.reader() );
    actual = new LineNumberReader( builder.actual.reader() );
  }

  private boolean compare() throws IOException {
    for ( ; ; ) {
      String expectedLine;
      for ( ; ; ) {
        expectedLine = expected.readLine();
        if ( expectedLine == null ) {
          break; }
        if ( builder.ignoreWhitespace ) {
          expectedLine = expectedLine.trim();
          if ( expectedLine.isEmpty() ) {
            continue; }
        }
        break;
      }
      String actualLine;
      for ( ; ; ) {
        actualLine = actual.readLine();
        if ( actualLine == null ) {
          break; }
        if ( builder.ignoreWhitespace ) {
          actualLine = actualLine.trim();
          if ( actualLine.isEmpty() ) {
            continue; }
        }
        if ( builder.filter != null ) {
          if ( ! builder.filter.accept( actualLine ) ) {
            continue; }
        }
        break;
      }
      if ( expectedLine == null  &&  actualLine == null ) {
        return true; }
      if ( expectedLine == null ) {
        System.err.println( "Missing lines at line " + expected.getLineNumber() );
        return false;
      }
      if ( actualLine == null ) {
        System.err.println( "Unexpected lines at line " + expected.getLineNumber() );
        return false;
      }
      if ( ! actualLine.equals( expectedLine ) ) {
        System.err.println( "Lines differ. Expected: " + expected.getLineNumber() +
                            ", Actual: " + actual.getLineNumber() );
        return false;
      }
    }
  }

  private void close() {
    Closeables.closeQuietly( expected );
    Closeables.closeQuietly( actual );
  }
}
